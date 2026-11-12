#!/usr/bin/env python3
"""
Fases 2 y 3: Pipeline de Saneamiento, Coherencia, Deteccion de Outliers (IQR)
y Perfilado de Calidad KDD bajo ISO/IEC 25012.
Modulo: Big Data Aplicado (BDA - RA3 & RA5 / CE 3.a, 3.c, 3.d, 5.a, 5.b)
Practica: 5075 - Pipeline de Integridad KDD
"""

import json
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


COLUMNAS_ESQUEMA = [
    "id_transaccion",
    "tienda",
    "timestamp",
    "categoria",
    "importe_eur",
    "unidades",
    "metodo_pago",
]
COLUMNAS_OBLIGATORIAS = ["id_transaccion", "timestamp", "importe_eur", "unidades"]
ANIO_EN_CURSO = 2026
FECHA_CORTE_LOTE = pd.Timestamp("2026-11-12 23:59:59")


def configurar_logger(ruta_log: Path) -> logging.Logger:
    """Configura salida dual en modo append para continuar la auditoria de Fase 1."""
    ruta_log.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("PipelineSaneamientoKDD")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formato = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(ruta_log, mode="a", encoding="utf-8")
    fh.setFormatter(formato)
    sh = logging.StreamHandler()
    sh.setFormatter(formato)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def ejecutar_pipeline(base_dir: Path) -> dict:
    """Ejecuta el saneamiento, segregacion en cuarentena y perfilado KDD."""
    staging_dir = base_dir / "staging" / "verified"
    quarantine_dir = base_dir / "quarantine"
    processed_dir = base_dir / "processed"
    reports_dir = base_dir / "reports"
    output_samples_dir = base_dir / "output_samples"
    log_path = base_dir / "logs" / "ejecucion_pipeline.log"

    for d in (quarantine_dir, processed_dir, reports_dir, output_samples_dir):
        d.mkdir(parents=True, exist_ok=True)

    logger = configurar_logger(log_path)
    logger.info("=== INICIO FASE 2: PIPELINE DE LIMPIEZA, COHERENCIA Y DETECCION DE OUTLIERS ===")

    archivos_staging = sorted(staging_dir.glob("*.csv"))
    if not archivos_staging:
        logger.critical("No hay ficheros verificados en staging/verified/. Ejecute Fase 1 primero.")
        raise RuntimeError("Staging vacio.")

    frames = []
    for f_csv in archivos_staging:
        df_tmp = pd.read_csv(
            f_csv,
            dtype={
                "id_transaccion": "string",
                "tienda": "string",
                "timestamp": "string",
                "categoria": "string",
                "metodo_pago": "string",
            },
        )
        if list(df_tmp.columns) != COLUMNAS_ESQUEMA:
            raise ValueError(f"Schema drift detectado en {f_csv.name}: {list(df_tmp.columns)}")
        df_tmp["archivo_origen"] = f_csv.name
        frames.append(df_tmp)
        logger.info(f"Cargado desde staging: {f_csv.name} ({len(df_tmp)} registros)")

    df_raw = pd.concat(frames, ignore_index=True)
    total_staging = len(df_raw)
    registros_sin_nulos_origen = int(df_raw[COLUMNAS_ESQUEMA].dropna().shape[0])
    tasa_completitud_origen = round((registros_sin_nulos_origen / total_staging) * 100, 2)

    cuarentena_lista = []

    def enviar_a_cuarentena(df_malo: pd.DataFrame, motivo: str) -> None:
        if df_malo.empty:
            return
        copia = df_malo.copy()
        copia["motivo_cuarentena"] = motivo
        cuarentena_lista.append(copia)
        for _, fila in copia.iterrows():
            id_tx = fila["id_transaccion"] if pd.notna(fila["id_transaccion"]) else "NULL_ID"
            logger.warning(
                f"[CUARENTENA] Origen={fila['archivo_origen']} | ID={id_tx} | Motivo={motivo}"
            )

    mask_nulos_oblig = df_raw[COLUMNAS_OBLIGATORIAS].isna().any(axis=1)
    for idx, fila in df_raw[mask_nulos_oblig].iterrows():
        cols_faltantes = [c for c in COLUMNAS_OBLIGATORIAS if pd.isna(fila[c])]
        enviar_a_cuarentena(
            df_raw.loc[[idx]],
            f"NULO_EN_COLUMNA_OBLIGATORIA: {', '.join(cols_faltantes)}",
        )
    df_valido = df_raw[~mask_nulos_oblig].copy()

    nulos_cat = int(df_valido["categoria"].isna().sum())
    nulos_mp = int(df_valido["metodo_pago"].isna().sum())
    if nulos_cat > 0:
        df_valido["categoria"] = df_valido["categoria"].fillna("No_Especificada")
        logger.info(f"Imputacion secundaria: {nulos_cat} nulos en 'categoria' -> 'No_Especificada'")
    if nulos_mp > 0:
        df_valido["metodo_pago"] = df_valido["metodo_pago"].fillna("No_Especificado")
        logger.info(f"Imputacion secundaria: {nulos_mp} nulos en 'metodo_pago' -> 'No_Especificado'")

    ts_parsed = pd.to_datetime(df_valido["timestamp"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    mask_ts_invalido = ts_parsed.isna()
    enviar_a_cuarentena(df_valido[mask_ts_invalido], "TIMESTAMP_CORRUPTO_NO_ISO8601")
    df_valido = df_valido[~mask_ts_invalido].copy()
    ts_parsed = ts_parsed[~mask_ts_invalido]

    mask_fuera_anio = (ts_parsed.dt.year != ANIO_EN_CURSO) | (ts_parsed > FECHA_CORTE_LOTE)
    for idx, fila in df_valido[mask_fuera_anio].iterrows():
        enviar_a_cuarentena(
            df_valido.loc[[idx]],
            f"FECHA_FUERA_DE_RANGO_{ANIO_EN_CURSO}: {fila['timestamp']}",
        )
    df_valido = df_valido[~mask_fuera_anio].copy()
    df_valido["timestamp"] = pd.to_datetime(df_valido["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    df_valido["importe_eur"] = pd.to_numeric(df_valido["importe_eur"], errors="coerce")
    df_valido["unidades"] = pd.to_numeric(df_valido["unidades"], errors="coerce")

    mask_dom_importe = df_valido["importe_eur"].isna() | (df_valido["importe_eur"] <= 0)
    for idx, fila in df_valido[mask_dom_importe].iterrows():
        enviar_a_cuarentena(
            df_valido.loc[[idx]],
            f"VIOLACION_DOMINIO: importe_eur <= 0 ({fila['importe_eur']})",
        )
    df_valido = df_valido[~mask_dom_importe].copy()

    mask_dom_unidades = df_valido["unidades"].isna() | (df_valido["unidades"] < 1)
    for idx, fila in df_valido[mask_dom_unidades].iterrows():
        enviar_a_cuarentena(
            df_valido.loc[[idx]],
            f"VIOLACION_DOMINIO: unidades < 1 ({fila['unidades']})",
        )
    df_valido = df_valido[~mask_dom_unidades].copy()

    mask_duplicados = df_valido.duplicated(subset=["id_transaccion"], keep="first")
    enviar_a_cuarentena(df_valido[mask_duplicados], "UNICIDAD_VIOLADA: id_transaccion duplicado")
    df_valido = df_valido[~mask_duplicados].copy()

    q1 = float(np.percentile(df_valido["importe_eur"], 25))
    q3 = float(np.percentile(df_valido["importe_eur"], 75))
    iqr = q3 - q1
    limite_inf = round(q1 - 1.5 * iqr, 4)
    limite_sup = round(q3 + 1.5 * iqr, 4)

    logger.info(
        f"Umbrales IQR importe_eur -> Q1={q1:.2f} | Q3={q3:.2f} | IQR={iqr:.2f} | "
        f"Limites=[{limite_inf:.2f}, {limite_sup:.2f}]"
    )

    mask_outliers = (df_valido["importe_eur"] < limite_inf) | (df_valido["importe_eur"] > limite_sup)
    for idx, fila in df_valido[mask_outliers].iterrows():
        enviar_a_cuarentena(
            df_valido.loc[[idx]],
            f"OUTLIER_ESTADISTICO_IQR: importe_eur={fila['importe_eur']} fuera de [{limite_inf:.2f}, {limite_sup:.2f}]",
        )
    df_limpio = df_valido[~mask_outliers].copy()

    df_limpio["importe_eur"] = df_limpio["importe_eur"].round(2)
    df_limpio["unidades"] = df_limpio["unidades"].astype(int)
    df_limpio = df_limpio.sort_values(by=["timestamp", "id_transaccion"]).reset_index(drop=True)

    df_bad = pd.concat(cuarentena_lista, ignore_index=True) if cuarentena_lista else pd.DataFrame()
    bad_records_path = quarantine_dir / "bad_records.csv"
    df_bad.to_csv(bad_records_path, index=False, encoding="utf-8")

    processed_path = processed_dir / "dataset_kdd_listo.csv"
    df_limpio[COLUMNAS_ESQUEMA].to_csv(processed_path, index=False, encoding="utf-8")
    logger.info(
        f"Dataset limpio exportado: {processed_path.relative_to(base_dir)} ({len(df_limpio)} registros conformes)"
    )
    logger.info(
        f"Registros rechazados exportados: {bad_records_path.relative_to(base_dir)} ({len(df_bad)} registros)"
    )
    logger.info("=== FIN FASE 2 ===\n")

    logger.info("=== INICIO FASE 3: PERFILADO KDD E INFORME DE CALIDAD (ISO/IEC 25012) ===")

    estado_f1_path = base_dir / "staging" / "estado_integridad_f1.json"
    with open(estado_f1_path, "r", encoding="utf-8") as jf:
        estado_f1 = json.load(jf)

    registros_finales = len(df_limpio)
    registros_sin_nulos_final = int(df_limpio[COLUMNAS_ESQUEMA].dropna().shape[0])
    tasa_completitud_final = round((registros_sin_nulos_final / registros_finales) * 100, 2)
    tasa_validez_staging = round((registros_finales / total_staging) * 100, 2)
    tasa_unicidad = round(
        (df_limpio["id_transaccion"].nunique() / registros_finales) * 100, 2
    )

    serie_imp = df_limpio["importe_eur"]
    stats_importe = {
        "media_eur": round(float(serie_imp.mean()), 2),
        "mediana_eur": round(float(serie_imp.median()), 2),
        "desviacion_estandar_eur": round(float(serie_imp.std(ddof=1)), 2),
        "percentil_25_eur": round(float(np.percentile(serie_imp, 25)), 2),
        "percentil_75_eur": round(float(np.percentile(serie_imp, 75)), 2),
        "minimo_eur": round(float(serie_imp.min()), 2),
        "maximo_eur": round(float(serie_imp.max()), 2),
    }

    serie_uni = df_limpio["unidades"]
    stats_unidades = {
        "media": round(float(serie_uni.mean()), 2),
        "mediana": round(float(serie_uni.median()), 2),
        "desviacion_estandar": round(float(serie_uni.std(ddof=1)), 2),
        "minimo": int(serie_uni.min()),
        "maximo": int(serie_uni.max()),
    }

    metricas_kdd = {
        "proyecto": "Practica_5075_Integridad_KDD",
        "norma_referencia": "ISO/IEC 25012",
        "fecha_ejecucion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "fase_1_integridad_criptografica": {
            "archivos_totales_lote": estado_f1["archivos_evaluados"],
            "archivos_validados": estado_f1["archivos_validados"],
            "archivos_rechazados": estado_f1["archivos_rechazados"],
            "tasa_integridad_criptografica_pct": estado_f1["tasa_integridad_criptografica_pct"],
        },
        "fase_2_saneamiento_y_cuarentena": {
            "registros_evaluados_staging": total_staging,
            "registros_conformes_finales": registros_finales,
            "registros_enviados_cuarentena": len(df_bad),
            "registros_imputados_secundarios": nulos_cat + nulos_mp,
        },
        "fase_3_indicadores_iso_25012": {
            "tasa_completitud_origen_staging_pct": tasa_completitud_origen,
            "tasa_completitud_post_saneamiento_pct": tasa_completitud_final,
            "tasa_unicidad_pct": tasa_unicidad,
            "tasa_validez_registros_staging_pct": tasa_validez_staging,
        },
        "distribucion_importe_eur_limpio": stats_importe,
        "distribucion_unidades_limpio": stats_unidades,
    }

    metricas_path = output_samples_dir / "metricas_perfilado.json"
    with open(metricas_path, "w", encoding="utf-8") as mf:
        json.dump(metricas_kdd, mf, indent=4, ensure_ascii=False)

    q_manifest_path = output_samples_dir / "quarantine_manifest.txt"
    with open(q_manifest_path, "a", encoding="utf-8") as qf:
        qf.write("[2] REGISTROS SEGREGADOS POR REGLAS DE CALIDAD Y OUTLIERS (FASE 2)\n")
        qf.write("--------------------------------------------------------------------\n")
        qf.write(f"Total registros en quarantine/bad_records.csv: {len(df_bad)}\n\n")
        for idx, r in df_bad.iterrows():
            id_tx = r["id_transaccion"] if pd.notna(r["id_transaccion"]) else "NULL_ID"
            qf.write(
                f"{idx + 1:02d}. [{r['archivo_origen']}] ID={id_tx} | "
                f"TS={r['timestamp']} | Importe={r['importe_eur']} | "
                f"Unidades={r['unidades']} -> {r['motivo_cuarentena']}\n"
            )

    logger.info(f"Metricas JSON guardadas en: {metricas_path.relative_to(base_dir)}")
    logger.info("=== FIN FASE 3 - PIPELINE COMPLETADO CON EXITO ===")
    return metricas_kdd


if __name__ == "__main__":
    raiz_proyecto = Path(__file__).resolve().parent.parent
    ejecutar_pipeline(raiz_proyecto)
