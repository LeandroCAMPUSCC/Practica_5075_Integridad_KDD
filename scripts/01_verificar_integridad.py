#!/usr/bin/env python3
"""
Fase 1: Pasarela de Integridad y Validacion Criptografica por Lotes (SHA-256).
Modulo: Big Data Aplicado (BDA - RA3 / CE 3.b)
Practica: 5075 - Pipeline de Integridad KDD
"""

import hashlib
import json
import logging
import shutil
from datetime import datetime
from pathlib import Path


def configurar_logger(ruta_log: Path) -> logging.Logger:
    """Configura salida dual (consola y fichero de auditoria) para trazabilidad."""
    ruta_log.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("PasarelaIntegridadSHA256")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formato = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(ruta_log, mode="w", encoding="utf-8")
    fh.setFormatter(formato)
    sh = logging.StreamHandler()
    sh.setFormatter(formato)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def calcular_sha256(ruta_fichero: Path, chunk_size: int = 65536) -> str:
    """Calcula el digest SHA-256 leyendo el archivo en bloques binarios de 64 KB."""
    hasher = hashlib.sha256()
    with open(ruta_fichero, "rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def verificar_lote(base_dir: Path) -> dict:
    """Contrasta cada fichero del lote contra manifest_checksums.sha256 y segrega."""
    raw_dir = base_dir / "raw_batch_202611"
    manifest_path = raw_dir / "manifest_checksums.sha256"
    staging_verified_dir = base_dir / "staging" / "verified"
    quarantine_tampered_dir = base_dir / "quarantine" / "tampered"
    output_samples_dir = base_dir / "output_samples"
    log_path = base_dir / "logs" / "ejecucion_pipeline.log"

    staging_verified_dir.mkdir(parents=True, exist_ok=True)
    quarantine_tampered_dir.mkdir(parents=True, exist_ok=True)
    output_samples_dir.mkdir(parents=True, exist_ok=True)

    logger = configurar_logger(log_path)
    logger.info("=== INICIO FASE 1: PASARELA DE INTEGRIDAD CRIPTOGRAFICA SHA-256 ===")
    logger.info(f"Manifiesto de control: {base_dir.name}/{manifest_path.relative_to(base_dir)}")

    if not manifest_path.exists():
        logger.critical(f"No se encuentra el manifiesto SHA-256 en {manifest_path}")
        raise FileNotFoundError(f"Manifiesto no encontrado: {manifest_path}")

    resultados = []
    validados = 0
    rechazados = 0

    with open(manifest_path, "r", encoding="utf-8") as f:
        lineas = [linea.strip() for linea in f if linea.strip()]

    for linea in lineas:
        partes = linea.split()
        if len(partes) != 2:
            logger.warning(f"Linea malformada en manifiesto omitida: {linea}")
            continue

        hash_esperado, nombre_archivo = partes[0].lower(), partes[1]
        ruta_origen = raw_dir / nombre_archivo

        if not ruta_origen.exists():
            logger.error(f"ALERTA: Archivo declarado en manifiesto no encontrado -> {nombre_archivo}")
            rechazados += 1
            resultados.append({
                "archivo": nombre_archivo,
                "estado": "NO_ENCONTRADO",
                "hash_esperado": hash_esperado,
                "hash_calculado": None,
                "destino": None,
            })
            continue

        hash_calculado = calcular_sha256(ruta_origen)

        if hash_calculado == hash_esperado:
            ruta_destino = staging_verified_dir / nombre_archivo
            shutil.copy2(ruta_origen, ruta_destino)
            validados += 1
            logger.info(
                f"[OK - INTEGRO] {nombre_archivo} | SHA-256={hash_calculado[:16]}... "
                f"-> Promovido a {ruta_destino.relative_to(base_dir)}"
            )
            resultados.append({
                "archivo": nombre_archivo,
                "estado": "VERIFICADO_OK",
                "hash_esperado": hash_esperado,
                "hash_calculado": hash_calculado,
                "destino": str(ruta_destino.relative_to(base_dir)),
            })
        else:
            ruta_cuarentena = quarantine_tampered_dir / nombre_archivo
            shutil.move(str(ruta_origen), str(ruta_cuarentena))
            rechazados += 1
            logger.error(
                f"[ALERTA SEGURIDAD - FIRMA DISCREPANTE] {nombre_archivo} | "
                f"Esperado={hash_esperado} | Calculado={hash_calculado} "
                f"-> Segregado a {ruta_cuarentena.relative_to(base_dir)}"
            )
            resultados.append({
                "archivo": nombre_archivo,
                "estado": "RECHAZADO_TAMPERED",
                "hash_esperado": hash_esperado,
                "hash_calculado": hash_calculado,
                "destino": str(ruta_cuarentena.relative_to(base_dir)),
            })

    total = validados + rechazados
    tasa_integridad = round((validados / total) * 100, 2) if total > 0 else 0.0

    resumen_f1 = {
        "timestamp_verificacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "archivos_evaluados": total,
        "archivos_validados": validados,
        "archivos_rechazados": rechazados,
        "tasa_integridad_criptografica_pct": tasa_integridad,
        "detalle_archivos": resultados,
    }

    estado_f1_path = base_dir / "staging" / "estado_integridad_f1.json"
    with open(estado_f1_path, "w", encoding="utf-8") as jf:
        json.dump(resumen_f1, jf, indent=4, ensure_ascii=False)

    q_manifest_path = output_samples_dir / "quarantine_manifest.txt"
    with open(q_manifest_path, "w", encoding="utf-8") as qf:
        qf.write("====================================================================\n")
        qf.write("MANIFIESTO DE CUARENTENA - PIPELINE DE INTEGRIDAD KDD (PRACTICA 5075)\n")
        qf.write(f"Fecha de generacion: {resumen_f1['timestamp_verificacion']}\n")
        qf.write("====================================================================\n\n")
        qf.write("[1] FICHEROS SEGREGADOS POR FALLO DE INTEGRIDAD CRIPTOGRAFICA (FASE 1)\n")
        qf.write("--------------------------------------------------------------------\n")
        for r in resultados:
            if r["estado"] != "VERIFICADO_OK":
                qf.write(f"- Archivo       : {r['archivo']}\n")
                qf.write(f"  Estado        : {r['estado']}\n")
                qf.write(f"  SHA-256 Esp.  : {r['hash_esperado']}\n")
                qf.write(f"  SHA-256 Calc. : {r['hash_calculado']}\n")
                qf.write(f"  Ubicacion     : {r['destino']}\n")
                qf.write("  Motivo        : Alteracion no autorizada o corrupcion en transito.\n\n")

    logger.info(
        f"Resumen Fase 1 -> Total: {total} | Validados: {validados} | "
        f"Rechazados: {rechazados} | Tasa Integridad: {tasa_integridad}%"
    )
    logger.info("=== FIN FASE 1 ===\n")
    return resumen_f1


if __name__ == "__main__":
    raiz_proyecto = Path(__file__).resolve().parent.parent
    verificar_lote(raiz_proyecto)
