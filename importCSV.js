const fs = require('fs');
const csv = require('csv-parser');
const { pool } = require('./config/db');

class CompleteSecopImporter {
    constructor() {
        this.totalProcessed = 0;
        this.totalSkipped = 0;
        this.totalErrors = 0;
        this.entidadCache = new Map();
        this.proveedorCache = new Map();
        this.startTime = Date.now();
        this.batchCount = 0;
        this.currentBatch = [];
        this.batchSize = 100;
        this.processing = false;
    }

    truncate(str, maxLength) {
        if (!str) return null;
        return str.toString().substring(0, maxLength);
    }

    parseNumber(numStr) {
        if (!numStr) return null;
        const cleaned = numStr.toString().replace(/[$,\s]/g, '');
        const num = parseFloat(cleaned);
        return isNaN(num) ? null : num;
    }

    parseDate(dateStr) {
        if (!dateStr) return null;
        try {
            if (dateStr.includes('/')) {
                const [day, month, year] = dateStr.split('/');
                const date = new Date(year, month - 1, day);
                return isNaN(date.getTime()) ? null : date;
            }
            const date = new Date(dateStr);
            return isNaN(date.getTime()) ? null : date;
        } catch {
            return null;
        }
    }

    normalizeBoolean(value) {
        if (!value) return false;
        const str = value.toString().toLowerCase().trim();
        return str === 'si' || str === 'sí' || str === 'true' || str === '1' || str === 'yes';
    }

    async getOrCreateEntidad(connection, data) {
        const nit = this.truncate(data.nit, 20);
        if (!nit) return null;

        const cacheKey = nit;
        if (this.entidadCache.has(cacheKey)) {
            return this.entidadCache.get(cacheKey);
        }

        try {
            const [existing] = await connection.execute(
                'SELECT EntidadId FROM Entidad WHERE nit_entidad = ?',
                [nit]
            );

            if (existing.length > 0) {
                this.entidadCache.set(cacheKey, existing[0].EntidadId);
                return existing[0].EntidadId;
            }

            const [result] = await connection.execute(`
                INSERT INTO Entidad (
                    nombre_entidad, nit_entidad, departamento, ciudad,
                    localizacion, orden, sector, rama, entidad_centralizada, codigo_entidad
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `, [
                this.truncate(data.nombre, 300),
                nit,
                this.truncate(data.departamento, 120),
                this.truncate(data.ciudad, 120),
                this.truncate(data.localizacion, 400),
                this.truncate(data.orden, 120),
                this.truncate(data.sector, 120),
                this.truncate(data.rama, 120),
                data.centralizada,
                data.codigo
            ]);

            this.entidadCache.set(cacheKey, result.insertId);
            return result.insertId;

        } catch (error) {
            console.error(`Error entidad ${nit}:`, error.message);
            return null;
        }
    }

    async getOrCreateProveedor(connection, data) {
        if (!data.nombre && !data.documento) return null;

        const documento = this.truncate(data.documento, 30);
        const codigo = this.truncate(data.codigo, 50);
        const nombre = this.truncate(data.nombre, 300);

        const cacheKey = documento || codigo || nombre;
        if (this.proveedorCache.has(cacheKey)) {
            return this.proveedorCache.get(cacheKey);
        }

        try {
            let query = 'SELECT ProveedorId FROM Proveedor WHERE ';
            let params = [];

            if (documento) {
                query += 'documento_proveedor = ?';
                params.push(documento);
            } else if (codigo) {
                query += 'codigo_proveedor = ?';
                params.push(codigo);
            } else {
                query += 'proveedor_adjudicado = ?';
                params.push(nombre);
            }

            const [existing] = await connection.execute(query, params);

            if (existing.length > 0) {
                this.proveedorCache.set(cacheKey, existing[0].ProveedorId);
                return existing[0].ProveedorId;
            }

            const [result] = await connection.execute(`
                INSERT INTO Proveedor (
                    codigo_proveedor, tipodocproveedor, documento_proveedor,
                    proveedor_adjudicado, es_grupo, es_pyme
                ) VALUES (?, ?, ?, ?, ?, ?)
            `, [
                codigo,
                this.truncate(data.tipodoc, 100),
                documento,
                nombre,
                data.esGrupo,
                data.esPyme
            ]);

            this.proveedorCache.set(cacheKey, result.insertId);
            return result.insertId;

        } catch (error) {
            console.error(`Error proveedor:`, error.message);
            return null;
        }
    }

    async insertRepresentanteLegal(connection, entidadId, data) {
        if (!data.nombre) return;

        try {
            await connection.execute(`
                INSERT IGNORE INTO RepresentanteLegal (
                    EntidadId, nombre_representante_legal, nacionalidad_representante_legal,
                    domicilio_representante_legal, tipo_de_identificacion_representante_legal,
                    identificacion_representante_legal, genero_representante_legal
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            `, [
                entidadId,
                this.truncate(data.nombre, 200),
                this.truncate(data.nacionalidad, 120),
                this.truncate(data.domicilio, 250),
                this.truncate(data.tipoId, 50),
                this.truncate(data.identificacion, 50),
                this.truncate(data.genero, 50)
            ]);
        } catch (error) {
        }
    }

    async processRecord(connection, row) {
        try {
            const nombreEntidad = row['Nombre Entidad']?.trim();
            const nitEntidad = row['Nit Entidad']?.toString().replace(/,/g, '').trim();
            const idContrato = row['ID Contrato']?.trim();

            if (!nombreEntidad || !nitEntidad || !idContrato) {
                this.totalSkipped++;
                return false;
            }

            const entidadData = {
                nombre: nombreEntidad,
                nit: nitEntidad,
                departamento: row['Departamento']?.trim(),
                ciudad: row['Ciudad']?.trim(),
                localizacion: row['Localización']?.trim(),
                orden: row['Orden']?.trim(),
                sector: row['Sector']?.trim(),
                rama: row['Rama']?.trim(),
                centralizada: this.normalizeBoolean(row['Entidad Centralizada']),
                codigo: this.parseNumber(row['Codigo Entidad'])
            };

            const proveedorData = {
                codigo: row['Codigo Proveedor']?.trim(),
                tipodoc: row['TipoDocProveedor']?.trim(),
                documento: row['Documento Proveedor']?.trim(),
                nombre: row['Proveedor Adjudicado']?.trim(),
                esGrupo: this.normalizeBoolean(row['Es Grupo']),
                esPyme: this.normalizeBoolean(row['Es Pyme'])
            };

            const representanteData = {
                nombre: row['Nombre Representante Legal']?.trim(),
                nacionalidad: row['Nacionalidad Representante Legal']?.trim(),
                domicilio: row['Domicilio Representante Legal']?.trim(),
                tipoId: row['Tipo de Identificación Representante Legal']?.trim(),
                identificacion: row['Identificación Representante Legal']?.trim(),
                genero: row['Género Representante Legal']?.trim()
            };

            const entidadId = await this.getOrCreateEntidad(connection, entidadData);
            if (!entidadId) {
                this.totalErrors++;
                return false;
            }

            await this.insertRepresentanteLegal(connection, entidadId, representanteData);

            const proveedorId = await this.getOrCreateProveedor(connection, proveedorData);

            const [contratoResult] = await connection.execute(`
                INSERT IGNORE INTO Contrato (
                    EntidadId, ProveedorId, proceso_de_compra, id_contrato,
                    referencia_del_contrato, estado_contrato, codigo_de_categoria_principal,
                    descripcion_del_proceso, tipo_de_contrato, modalidad_de_contratacion,
                    justificacion_modalidad_de, fecha_de_firma, fecha_de_inicio_del_contrato,
                    fecha_de_fin_del_contrato, fecha_de_inicio_de_ejecucion, fecha_de_fin_de_ejecucion,
                    condiciones_de_entrega, habilita_pago_adelantado, liquidacion,
                    obligacion_ambiental, obligaciones_postconsumo, reversion,
                    origen_de_los_recursos, destino_gasto, estado_bpin, codigo_bpin,
                    anno_bpin, espostconflicto, dias_adicionados, puntos_del_acuerdo,
                    pilares_del_acuerdo, urlproceso, ultima_actualizacion,
                    fecha_inicio_liquidacion, fecha_fin_liquidacion, objeto_del_contrato,
                    duracion_del_contrato, el_contrato_puede_ser_prorrogado,
                    fecha_de_notificacion_de_prorrogacion
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `, [
                entidadId,
                proveedorId,
                this.truncate(row['Proceso de Compra']?.trim(), 80),
                this.truncate(idContrato, 80),
                this.truncate(row['Referencia del Contrato']?.trim(), 120),
                this.truncate(row['Estado Contrato']?.trim(), 80),
                this.truncate(row['Codigo de Categoria Principal']?.trim(), 32),
                this.truncate(row['Descripcion del Proceso']?.trim(), 1000),
                this.truncate(row['Tipo de Contrato']?.trim(), 120),
                this.truncate(row['Modalidad de Contratacion']?.trim(), 200),
                this.truncate(row['Justificacion Modalidad de Contratacion']?.trim(), 1000),
                this.parseDate(row['Fecha de Firma']),
                this.parseDate(row['Fecha de Inicio del Contrato']),
                this.parseDate(row['Fecha de Fin del Contrato']),
                this.parseDate(row['Fecha de Inicio de Ejecucion']),
                this.parseDate(row['Fecha de Fin de Ejecucion']),
                this.truncate(row['Condiciones de Entrega']?.trim(), 500),
                this.normalizeBoolean(row['Habilita Pago Adelantado']),
                this.normalizeBoolean(row['Liquidación']),
                this.normalizeBoolean(row['Obligación Ambiental']),
                this.normalizeBoolean(row['Obligaciones Postconsumo']),
                this.normalizeBoolean(row['Reversion']),
                this.truncate(row['Origen de los Recursos']?.trim(), 200),
                this.truncate(row['Destino Gasto']?.trim(), 200),
                this.truncate(row['Estado BPIN']?.trim(), 80),
                this.truncate(row['Código BPIN']?.trim(), 50),
                this.truncate(row['Anno BPIN']?.trim(), 10),
                this.normalizeBoolean(row['EsPostConflicto']),
                this.parseNumber(row['Dias adicionados']),
                this.truncate(row['Puntos del Acuerdo']?.trim(), 300),
                this.truncate(row['Pilares del Acuerdo']?.trim(), 300),
                this.truncate(row['URLProceso']?.trim(), 500),
                this.parseDate(row['Ultima Actualizacion']),
                this.parseDate(row['Fecha Inicio Liquidacion']),
                this.parseDate(row['Fecha Fin Liquidacion']),
                this.truncate(row['Objeto del Contrato']?.trim(), 1000),
                this.truncate(row['Duración del contrato']?.trim(), 200),
                this.normalizeBoolean(row['El contrato puede ser prorrogado']),
                this.parseDate(row['Fecha de notificación de prorrogación'])
            ]);

            const contratoId = contratoResult.insertId;
            if (contratoId === 0) return false; 

            await connection.execute(`
                INSERT IGNORE INTO ContratoFinanzas (
                    ContratoId, valor_del_contrato, valor_de_pago_adelantado,
                    valor_facturado, valor_pendiente_de_pago, valor_pagado,
                    valor_amortizado, valor_pendiente_de_amortizacion,
                    valor_pendiente_de_ejecucion, saldo_cdp, saldo_vigencia
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `, [
                contratoId,
                this.parseNumber(row['Valor del Contrato']),
                this.parseNumber(row['Valor de pago adelantado']),
                this.parseNumber(row['Valor Facturado']),
                this.parseNumber(row['Valor Pendiente de Pago']),
                this.parseNumber(row['Valor Pagado']),
                this.parseNumber(row['Valor Amortizado']),
                this.parseNumber(row['Valor Pendiente de Amortizacion']),
                this.parseNumber(row['Valor Pendiente de Ejecucion']),
                this.parseNumber(row['Saldo CDP']),
                this.parseNumber(row['Saldo Vigencia'])
            ]);

            await connection.execute(`
                INSERT IGNORE INTO ContratoRecursos (
                    ContratoId, presupuesto_general_de_la_nacion_pgn, sistema_general_de_participaciones,
                    sistema_general_de_regalias, recursos_propios_alcaldias_gobernaciones_resguardos,
                    recursos_de_credito, recursos_propios
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            `, [
                contratoId,
                this.parseNumber(row['Presupuesto General de la Nacion – PGN']),
                this.parseNumber(row['Sistema General de Participaciones']),
                this.parseNumber(row['Sistema General de Regalías']),
                this.parseNumber(row['Recursos Propios (Alcaldías, Gobernaciones y Resguardos Indígenas)']),
                this.parseNumber(row['Recursos de Credito']),
                this.parseNumber(row['Recursos Propios'])
            ]);

            if (row['Nombre del banco']) {
                await connection.execute(`
                    INSERT IGNORE INTO ContratoBancario (ContratoId, nombre_del_banco, tipo_de_cuenta, numero_de_cuenta)
                    VALUES (?, ?, ?, ?)
                `, [
                    contratoId,
                    this.truncate(row['Nombre del banco']?.trim(), 200),
                    this.truncate(row['Tipo de cuenta']?.trim(), 50),
                    this.truncate(row['Número de cuenta']?.trim(), 50)
                ]);
            }

            const responsables = [
                {
                    rol: 'Supervisor',
                    nombre: row['Nombre supervisor']?.trim(),
                    tipoDoc: row['Tipo de documento supervisor']?.trim(),
                    numeroDoc: row['Número de documento supervisor']?.trim()
                },
                {
                    rol: 'OrdenadorGasto',
                    nombre: row['Nombre ordenador del gasto']?.trim(),
                    tipoDoc: row['Tipo de documento Ordenador del gasto']?.trim(),
                    numeroDoc: row['Número de documento Ordenador del gasto']?.trim()
                },
                {
                    rol: 'OrdenadorPago',
                    nombre: row['Nombre Ordenador de Pago']?.trim(),
                    tipoDoc: row['Tipo de documento Ordenador de Pago']?.trim(),
                    numeroDoc: row['Número de documento Ordenador de Pago']?.trim()
                }
            ];

            for (const resp of responsables) {
                if (resp.nombre) {
                    await connection.execute(`
                        INSERT IGNORE INTO ContratoResponsable (ContratoId, rol, nombre, tipo_documento, numero_documento)
                        VALUES (?, ?, ?, ?, ?)
                    `, [
                        contratoId,
                        resp.rol,
                        this.truncate(resp.nombre, 200),
                        this.truncate(resp.tipoDoc, 40),
                        this.truncate(resp.numeroDoc, 40)
                    ]);
                }
            }

            this.totalProcessed++;
            return true;

        } catch (error) {
            this.totalErrors++;
            if (this.totalErrors <= 5) {
                console.error(`Error completo:`, error.message);
            }
            return false;
        }
    }

    async processBatch() {
        if (this.currentBatch.length === 0 || this.processing) return;

        this.processing = true;
        this.batchCount++;

        const connection = await pool.getConnection();
        await connection.beginTransaction();

        try {
            for (const record of this.currentBatch) {
                await this.processRecord(connection, record);
            }

            await connection.commit();
            this.showProgress();

        } catch (error) {
            await connection.rollback();
            console.error(`Error en lote ${this.batchCount}:`, error.message);
        } finally {
            connection.release();
            this.currentBatch = [];
            this.processing = false;
        }
    }

    showProgress() {
        const elapsed = (Date.now() - this.startTime) / 1000;
        const rate = this.totalProcessed / elapsed;
        
        if (this.batchCount % 5 === 0) {
            if (this.entidadCache.size > 2000) {
                this.entidadCache.clear();
            }
            if (this.proveedorCache.size > 2000) {
                this.proveedorCache.clear();
            }
        }

        console.log(`Lote ${this.batchCount}: ${this.totalProcessed.toLocaleString()} procesados | ${this.totalSkipped.toLocaleString()} omitidos | ${this.totalErrors.toLocaleString()} errores | ${rate.toFixed(1)} reg/seg`);
    }

    async importCSV(filePath) {
        console.log(`Iniciando importación COMPLETA (8 tablas): ${filePath}`);

        return new Promise((resolve, reject) => {
            let totalRows = 0;

            const stream = fs.createReadStream(filePath)
                .pipe(csv({ separator: ',', skipEmptyLines: true }))
                .on('data', async (row) => {
                    totalRows++;
                    this.currentBatch.push(row);

                    if (this.currentBatch.length >= this.batchSize) {
                        stream.pause();
                        await this.processBatch();
                        stream.resume();
                    }

                    if (totalRows % 10000 === 0) {
                        console.log(`Leidos: ${totalRows.toLocaleString()} registros`);
                    }
                })
                .on('end', async () => {
                    if (this.currentBatch.length > 0) {
                        await this.processBatch();
                    }

                    console.log('Importación COMPLETA terminada');
                    resolve({
                        totalProcessed: this.totalProcessed,
                        totalSkipped: this.totalSkipped,
                        totalErrors: this.totalErrors,
                        totalRows
                    });
                })
                .on('error', reject);
        });
    }
}

async function main() {
    const csvFile = './SECOP_II_-_Contratos_Electrónicos_20250907.csv';
    
    if (!fs.existsSync(csvFile)) {
        console.error(`Archivo no encontrado: ${csvFile}`);
        process.exit(1);
    }

    console.log('IMPORTADOR COMPLETO: Usa las 8 tablas con TODOS los campos disponibles');

    const importer = new CompleteSecopImporter();
    
    try {
        await pool.execute('SET SESSION foreign_key_checks = 0');
        await pool.execute('SET SESSION unique_checks = 0');
        await pool.execute('SET SESSION autocommit = 0');

        const result = await importer.importCSV(csvFile);

        await pool.execute('SET SESSION foreign_key_checks = 1');
        await pool.execute('SET SESSION unique_checks = 1');
        await pool.execute('SET SESSION autocommit = 1');

        console.log(`\n=== RESUMEN FINAL COMPLETO ===`);
        console.log(`Total registros leidos: ${result.totalRows.toLocaleString()}`);
        console.log(`Procesados exitosamente: ${result.totalProcessed.toLocaleString()}`);
        console.log(`Omitidos (sin datos): ${result.totalSkipped.toLocaleString()}`);
        console.log(`Errores: ${result.totalErrors.toLocaleString()}`);
        console.log(`Efectividad: ${((result.totalProcessed / result.totalRows) * 100).toFixed(2)}%`);
        console.log(`\nTablas utilizadas: TODAS (8 tablas)`);
        console.log(`- Entidad + RepresentanteLegal`);
        console.log(`- Proveedor`);
        console.log(`- Contrato + ContratoFinanzas + ContratoRecursos + ContratoBancario + ContratoResponsable`);

    } catch (error) {
        console.error('Error durante importación:', error);
    } finally {
        await pool.end();
    }
}

if (require.main === module) {
    main();
}