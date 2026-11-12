# Informe Técnico de Calidad de Datos y Preparación KDD (ISO/IEC 25012)

**Módulo:** Big Data Aplicado (BDA) — RA3 y RA5  
**Actividad:** Práctica 5075 · Pipeline de Integridad Criptográfica, Saneamiento y Perfilado KDD  
**Lote Analizado:** `raw_batch_202611` (12 de noviembre de 2026)  

---

## 1. Resumen Ejecutivo de Admisión por Fases

El pipeline automatizado actuó como pasarela de calidad (*Data Quality Gate*) previa al almacén de *Business Intelligence* (BI), aislando las incidencias en zonas de cuarentena sin interrumpir el flujo operativo:

| Fase del Pipeline | Entrada | Salida Conforme | Segregado a Cuarentena | Tasa de Aprobación |
| :--- | :---: | :---: | :---: | :---: |
| **Fase 1: Integridad SHA-256** | 3 ficheros (150 reg.) | 2 ficheros (`norte`, `este`) | 1 fichero (`sur` en `quarantine/tampered/`) | **66,67 %** |
| **Fase 2: Saneamiento y Outliers** | 100 registros (`staging`) | 87 registros (`dataset_kdd_listo.csv`) | 13 registros (`quarantine/bad_records.csv`) | **87,00 %** |
| **Fase 3: Consolidación KDD (BI)** | 87 registros limpios | 100 % esquematizados e ISO 8601 | 0 registros pendientes | **100,00 %** |

---

## 2. Auditoría Criptográfica y Control de Manipulación (Fase 1)

La verificación mediante `SHA-256` contra `manifest_checksums.sha256` detectó una discrepancia crítica antes de abrir los ficheros en memoria analítica:

* **`transacciones_tienda_norte.csv` (50 reg.):** Firma coincidente (`34cc6161...d86a549`). Promovido a `staging/verified/`.
* **`transacciones_tienda_este.csv` (50 reg.):** Firma coincidente (`645e9de2...3cd4dc9`). Promovido a `staging/verified/`.
* **`transacciones_tienda_sur.csv` (50 reg.):** **Rechazado**. Hash esperado `0773c2cd...128b83c` vs. calculado `1c47dff4...9986805`. La inspección forense posterior confirmó inyección no autorizada (`ALTERADO_NO_AUTORIZADO` en `TXS-20261110-0020` e importe ficticio `9999.99` en `TXS-20261110-0005`).

---

## 3. Métricas de Calidad ISO/IEC 25012 y Perfilado Estadístico (Fase 3)

### 3.1 Indicadores de Calidad del Dato

| Dimensión ISO/IEC 25012 | Métrica Aplicada | Valor Pre-Limpieza (`staging`) | Valor Post-Limpieza (`processed`) |
| :--- | :--- | :---: | :---: |
| **Integridad / Confidencialidad** | Tasa de Integridad Criptográfica SHA-256 | 66,67 % (2/3 ficheros) | 100,00 % (sobre ficheros admitidos) |
| **Completitud** | $\frac{\text{Registros sin nulos}}{\text{Registros totales}} \times 100$ | 95,00 % (95/100 registros) | **100,00 %** (87/87 registros) |
| **Unicidad** | Claves primarias `id_transaccion` únicas | 100,00 % (sobre IDs no nulos) | **100,00 %** (87/87 IDs únicos) |
| **Exactitud y Validez** | Conformidad temporal y de dominio | 87,00 % (87/100 registros) | **100,00 %** (87/87 conformes) |

### 3.2 Distribución de Variables Clave (`processed/dataset_kdd_listo.csv`)

Tras depurar los 13 registros anómalos de `transacciones_tienda_este.csv`, la variable monetaria `importe_eur` presenta una distribución simétrica y libre de sesgos extremos:

| Estadístico Descriptivo | `importe_eur` (EUR) | `unidades` (ud.) |
| :--- | :---: | :---: |
| **Media ($\mu$)** | **70,99 €** | **3,79** |
| **Mediana (Percentil 50)** | **73,44 €** | **4,00** |
| **Desviación Estándar ($s$)** | **26,34 €** | **1,69** |
| **Percentil 25 ($Q_1$)** | **53,80 €** | **2,00** |
| **Percentil 75 ($Q_3$)** | **85,34 €** | **5,00** |
| **Rango [Mínimo – Máximo]** | **[12,50 € – 124,47 €]** | **[1 – 6]** |

---

## 4. Conclusiones Técnicas y Justificación de Decisiones KDD

1. **Segregación estricta frente a imputación en claves y métricas críticas:** Los registros sin `id_transaccion` (2 casos) o sin `importe_eur` (`TXE-20261110-0034`) fueron enviados a `quarantine/bad_records.csv`. Imputar un identificador primario rompería la trazabilidad transaccional, mientras que imputar un importe monetario por la mediana introduciría ruido artificial en los KPIs de facturación del almacén de BI.
2. **Imputación neutra en atributos categóricos secundarios:** En `TXE-20261110-0007` (`categoria` nula) y `TXE-20261110-0015` (`metodo_pago` nulo), dado que la transacción financiera era íntegra y válida, se imputaron etiquetas explícitas (`No_Especificada` / `No_Especificado`). Esto conserva el 100 % de los ingresos reales sin inflar artificialmente la cuota de la categoría modal.
3. **Filtrado de coherencia temporal y dominio:** Se aislaron 1 marca temporal corrupta (`2026-99-99 INVALID_TS`), 2 fechas fuera del ejercicio `2026` (`2024` y `2027`) y 4 violaciones de dominio (`importe_eur <= 0` y `unidades < 1`), evitando fallos de particionado temporal en el *Data Warehouse* y distorsiones en el cálculo del ticket medio.
4. **Control de *outliers* por cercas de Tukey (IQR):** Las transacciones `TXE-20261110-0016` (`1480,00 €`), `TXE-20261110-0032` (`2750,90 €`) y `TXE-20261110-0047` (`4120,00 €`) superaban entre 11 y 30 veces el límite superior IQR (`138,61 €`). Su aislamiento evita que algoritmos posteriores de *clustering* (K-Means) o regresión en la fase de minería de datos KDD desplacen sus centroides hacia lecturas erróneas de sensores/TPV.