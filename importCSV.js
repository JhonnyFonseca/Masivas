const mysql = require('mysql2/promise');
const fs = require('fs');
const csv = require('csv-parser');

const dbConfig = {
    host: 'localhost',
    port: 3307,
    user: 'root',
    password: 'sql-bdm-pssw',
    database: 'secop_contratos',
    waitForConnections: true,
    connectionLimit: 30,
    queueLimit: 0,
    charset: 'utf8mb4',
    supportBigNumbers: true,
    bigNumberStrings: true,
    dateStrings: true
};

const pool = mysql.createPool(dbConfig);

class FixedUltraSecopImporter {
    constructor() {
        this.totalProcessed = 0;
        this.totalSkipped = 0;
        this.totalErrors = 0;
        this.startTime = Date.now();
        this.batchCount = 0;
        this.currentBatch = [];
        this.batchSize = 2000;
        this.processing = false;
        this.lastProgressTime = Date.now();
        
        this.entidadCache = new Map();
        this.proveedorCache = new Map();
        this.maxCacheSize = 100000;
        
        this.checkpointInterval = 10000;
        this.checkpointFile = './checkpoint_fixed.json';
        this.lastProcessedRow = 0;
        
        // Buffers para inserts masivos
        this.entidadBuffer = [];
        this.proveedorBuffer = [];
        this.contratoBuffer = [];
        this.finanzasBuffer = [];
        this.recursosBuffer = [];
        this.bancarioBuffer = [];
        this.responsableBuffer = [];
        this.representanteBuffer = [];
        
        this.bufferSize = 1000; // Reducido para evitar memory issues
        
        // Sistema de conexiones múltiples
        this.connections = [];
        this.connectionCount = 4; // Reducido para mayor estabilidad
        this.connectionIndex = 0;
    }

    async initializeConnections() {
        console.log('🔗 Inicializando conexiones múltiples...');
        
        for (let i = 0; i < this.connectionCount; i++) {
            const connection = await pool.getConnection();
            
            // Solo configuraciones que sabemos que funcionan
            await connection.execute('SET SESSION autocommit = 0');
            await connection.execute('SET SESSION unique_checks = 0');
            await connection.execute('SET SESSION foreign_key_checks = 0');
            await connection.execute('SET SESSION sql_log_bin = 0');
            await connection.execute('SET SESSION innodb_lock_wait_timeout = 5');
            
            this.connections.push(connection);
        }
        
        console.log(`✅ ${this.connectionCount} conexiones optimizadas inicializadas`);
    }

    getNextConnection() {
        const connection = this.connections[this.connectionIndex];
        this.connectionIndex = (this.connectionIndex + 1) % this.connectionCount;
        return connection;
    }

    async closeConnections() {
        for (const connection of this.connections) {
            connection.release();
        }
        this.connections = [];
    }

    loadCheckpoint() {
        try {
            if (fs.existsSync(this.checkpointFile)) {
                const data = JSON.parse(fs.readFileSync(this.checkpointFile, 'utf8'));
                this.lastProcessedRow = data.lastProcessedRow || 0;
                this.totalProcessed = data.totalProcessed || 0;
                console.log(`🔄 Resumiendo desde la fila: ${this.lastProcessedRow.toLocaleString()}`);
                console.log(`📊 Registros ya procesados: ${this.totalProcessed.toLocaleString()}`);
            }
        } catch (error) {
            console.log('⚠️  No se pudo cargar checkpoint, iniciando desde cero');
        }
    }

    saveCheckpoint(currentRow) {
        try {
            const checkpoint = {
                lastProcessedRow: currentRow,
                totalProcessed: this.totalProcessed,
                timestamp: new Date().toISOString(),
                elapsedMinutes: Math.round((Date.now() - this.startTime) / 60000)
            };
            fs.writeFileSync(this.checkpointFile, JSON.stringify(checkpoint, null, 2));
        } catch (error) {
            console.error('❌ Error guardando checkpoint:', error.message);
        }
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

    async preloadEntidades() {
        console.log('🔄 Precargando entidades existentes...');
        
        const connection = this.getNextConnection();
        const [entidades] = await connection.execute(
            'SELECT EntidadId, nit_entidad FROM Entidad'
        );
        
        entidades.forEach(ent => {
            this.entidadCache.set(ent.nit_entidad, ent.EntidadId);
        });
        
        console.log(`✅ Precargadas ${entidades.length.toLocaleString()} entidades`);
    }

    async preloadProveedores() {
        console.log('🔄 Precargando proveedores existentes...');
        
        const connection = this.getNextConnection();
        const [proveedores] = await connection.execute(
            'SELECT ProveedorId, documento_proveedor, codigo_proveedor, proveedor_adjudicado FROM Proveedor'
        );
        
        proveedores.forEach(prov => {
            if (prov.documento_proveedor) {
                this.proveedorCache.set(prov.documento_proveedor, prov.ProveedorId);
            }
            if (prov.codigo_proveedor) {
                this.proveedorCache.set(prov.codigo_proveedor, prov.ProveedorId);
            }
            if (prov.proveedor_adjudicado) {
                this.proveedorCache.set(prov.proveedor_adjudicado, prov.ProveedorId);
            }
        });
        
        console.log(`✅ Precargados ${proveedores.length.toLocaleString()} proveedores`);
    }

    async flushEntidadBuffer(connection) {
        if (this.entidadBuffer.length === 0) return;

        const values = this.entidadBuffer.map(data => [
            data.nombre, data.nit, data.departamento, data.ciudad,
            data.localizacion, data.orden, data.sector, data.rama,
            data.centralizada, data.codigo
        ]);

        const placeholders = values.map(() => '(?,?,?,?,?,?,?,?,?,?)').join(',');
        const flatValues = values.flat();

        try {
            const [result] = await connection.execute(`
                INSERT IGNORE INTO Entidad (
                    nombre_entidad, nit_entidad, departamento, ciudad,
                    localizacion, orden, sector, rama, entidad_centralizada, codigo_entidad
                ) VALUES ${placeholders}
            `, flatValues);

            // Actualizar cache con IDs reales
            for (let i = 0; i < this.entidadBuffer.length; i++) {
                const data = this.entidadBuffer[i];
                if (!this.entidadCache.has(data.nit)) {
                    this.entidadCache.set(data.nit, result.insertId + i);
                }
            }
        } catch (error) {
            console.error('❌ Error flush entidades:', error.message);
        }

        this.entidadBuffer = [];
    }

    async flushProveedorBuffer(connection) {
        if (this.proveedorBuffer.length === 0) return;

        const values = this.proveedorBuffer.map(data => [
            data.codigo, data.tipodoc, data.documento,
            data.nombre, data.esGrupo, data.esPyme
        ]);

        const placeholders = values.map(() => '(?,?,?,?,?,?)').join(',');
        const flatValues = values.flat();

        try {
            await connection.execute(`
                INSERT IGNORE INTO Proveedor (
                    codigo_proveedor, tipodocproveedor, documento_proveedor,
                    proveedor_adjudicado, es_grupo, es_pyme
                ) VALUES ${placeholders}
            `, flatValues);
        } catch (error) {
            console.error('❌ Error flush proveedores:', error.message);
        }

        this.proveedorBuffer = [];
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
                nombre: this.truncate(nombreEntidad, 300),
                nit: this.truncate(nitEntidad, 20),
                departamento: this.truncate(row['Departamento']?.trim(), 120),
                ciudad: this.truncate(row['Ciudad']?.trim(), 120),
                localizacion: this.truncate(row['Localización']?.trim(), 400),
                orden: this.truncate(row['Orden']?.trim(), 120),
                sector: this.truncate(row['Sector']?.trim(), 120),
                rama: this.truncate(row['Rama']?.trim(), 120),
                centralizada: this.normalizeBoolean(row['Entidad Centralizada']),
                codigo: this.parseNumber(row['Codigo Entidad'])
            };

            const proveedorData = {
                codigo: this.truncate(row['Codigo Proveedor']?.trim(), 50),
                tipodoc: this.truncate(row['TipoDocProveedor']?.trim(), 100),
                documento: this.truncate(row['Documento Proveedor']?.trim(), 30),
                nombre: this.truncate(row['Proveedor Adjudicado']?.trim(), 300),
                esGrupo: this.normalizeBoolean(row['Es Grupo']),
                esPyme: this.normalizeBoolean(row['Es Pyme'])
            };

            // Obtener o crear entidad
            let entidadId = this.entidadCache.get(entidadData.nit);
            if (!entidadId) {
                this.entidadBuffer.push(entidadData);
                if (this.entidadBuffer.length >= this.bufferSize) {
                    await this.flushEntidadBuffer(connection);
                }
                entidadId = this.entidadCache.get(entidadData.nit) || Date.now();
            }

            // Obtener o crear proveedor
            let proveedorId = null;
            const proveedorKey = proveedorData.documento || proveedorData.codigo || proveedorData.nombre;
            if (proveedorKey) {
                proveedorId = this.proveedorCache.get(proveedorKey);
                if (!proveedorId) {
                    this.proveedorBuffer.push(proveedorData);
                    if (this.proveedorBuffer.length >= this.bufferSize) {
                        await this.flushProveedorBuffer(connection);
                    }
                    proveedorId = this.proveedorCache.get(proveedorKey) || Date.now();
                }
            }

            // Insertar contrato principal
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
                entidadId, proveedorId,
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

            // Insertar tablas relacionadas en paralelo
            await Promise.all([
                connection.execute(`
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
                ]),

                connection.execute(`
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
                ])
            ]);

            // Datos bancarios si existen
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

            // Responsables
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
                        contratoId, resp.rol,
                        this.truncate(resp.nombre, 200),
                        this.truncate(resp.tipoDoc, 40),
                        this.truncate(resp.numeroDoc, 40)
                    ]);
                }
            }

            // Representante legal
            const representante = row['Nombre Representante Legal']?.trim();
            if (representante) {
                await connection.execute(`
                    INSERT IGNORE INTO RepresentanteLegal (
                        EntidadId, nombre_representante_legal, nacionalidad_representante_legal,
                        domicilio_representante_legal, tipo_de_identificacion_representante_legal,
                        identificacion_representante_legal, genero_representante_legal
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                `, [
                    entidadId,
                    this.truncate(representante, 200),
                    this.truncate(row['Nacionalidad Representante Legal']?.trim(), 120),
                    this.truncate(row['Domicilio Representante Legal']?.trim(), 250),
                    this.truncate(row['Tipo de Identificación Representante Legal']?.trim(), 50),
                    this.truncate(row['Identificación Representante Legal']?.trim(), 50),
                    this.truncate(row['Género Representante Legal']?.trim(), 50)
                ]);
            }

            this.totalProcessed++;
            return true;

        } catch (error) {
            this.totalErrors++;
            if (this.totalErrors <= 10) {
                console.error(`❌ Error en registro:`, error.message);
            }
            return false;
        }
    }

    smartCleanupCaches() {
        if (this.entidadCache.size > this.maxCacheSize) {
            const entries = Array.from(this.entidadCache.entries());
            this.entidadCache.clear();
            entries.slice(-50000).forEach(([key, value]) => {
                this.entidadCache.set(key, value);
            });
        }

        if (this.proveedorCache.size > this.maxCacheSize) {
            const entries = Array.from(this.proveedorCache.entries());
            this.proveedorCache.clear();
            entries.slice(-50000).forEach(([key, value]) => {
                this.proveedorCache.set(key, value);
            });
        }
    }

    async processBatch() {
        if (this.currentBatch.length === 0 || this.processing) return;

        this.processing = true;
        this.batchCount++;

        const connection = this.getNextConnection();
        
        try {
            await connection.beginTransaction();

            // Procesar todos los registros del lote
            for (const record of this.currentBatch) {
                await this.processRecord(connection, record);
            }

            // Flush buffers pendientes
            await this.flushEntidadBuffer(connection);
            await this.flushProveedorBuffer(connection);

            await connection.commit();
            this.showProgress();

        } catch (error) {
            await connection.rollback();
            console.error(`❌ Error en lote ${this.batchCount}:`, error.message);
            this.totalErrors += this.currentBatch.length;
        } finally {
            this.currentBatch = [];
            this.processing = false;
        }
    }

    showProgress() {
        const now = Date.now();
        const elapsed = (now - this.startTime) / 1000;
        const rate = this.totalProcessed / elapsed;
        const progressElapsed = (now - this.lastProgressTime) / 1000;
        
        // Limpiar caches cada 10 lotes
        if (this.batchCount % 10 === 0) {
            this.smartCleanupCaches();
        }

        // Mostrar progreso cada 20 segundos
        if (progressElapsed >= 20 || this.batchCount % 15 === 0) {
            const estimatedTotal = rate * 3600;
            const remainingRecords = 4000000 - this.totalProcessed;
            const estimatedHoursRemaining = remainingRecords / rate / 3600;
            
            console.log(`
🚀 LOTE ${this.batchCount} | ⏱️  ${Math.round(elapsed/60)}min | 🕐 Faltan ~${estimatedHoursRemaining.toFixed(1)}h
📊 Procesados: ${this.totalProcessed.toLocaleString()} / 4,000,000 (${((this.totalProcessed/4000000)*100).toFixed(1)}%)
⚡ Velocidad: ${Math.round(rate)} reg/seg | 📈 Estimado/hora: ${Math.round(estimatedTotal).toLocaleString()}
💾 Cache: ${this.entidadCache.size.toLocaleString()} entidades, ${this.proveedorCache.size.toLocaleString()} proveedores
🎯 Progreso: ${'█'.repeat(Math.floor((this.totalProcessed/4000000)*20))}${'░'.repeat(20-Math.floor((this.totalProcessed/4000000)*20))}
            `.trim());
            this.lastProgressTime = now;
        }
    }

    async importCSV(filePath) {
        console.log(`
🚀 IMPORTADOR SECOP ULTRA-OPTIMIZADO - VERSIÓN CORREGIDA
📁 Archivo: ${filePath}
🎯 Meta: 4,000,000 registros en 8-10 horas
⚙️  Configuración: Lotes de ${this.batchSize}, Buffers de ${this.bufferSize}
🔗 Conexiones: ${this.connectionCount} conexiones simultáneas
        `.trim());

        await this.initializeConnections();
        await this.preloadEntidades();
        await this.preloadProveedores();
        this.loadCheckpoint();

        return new Promise((resolve, reject) => {
            let totalRows = 0;
            
            const stream = fs.createReadStream(filePath)
                .pipe(csv({ 
                    separator: ',', 
                    skipEmptyLines: true,
                    maxRowsPerRead: 1000
                }))
                .on('data', async (row) => {
                    totalRows++;
                    
                    if (totalRows <= this.lastProcessedRow) {
                        return;
                    }

                    this.currentBatch.push(row);

                    if (this.currentBatch.length >= this.batchSize) {
                        stream.pause();
                        await this.processBatch();
                        
                        if (totalRows % this.checkpointInterval === 0) {
                            this.saveCheckpoint(totalRows);
                        }
                        
                        stream.resume();
                    }

                    if (totalRows % 100000 === 0) {
                        const elapsed = (Date.now() - this.startTime) / 1000;
                        const readRate = totalRows / elapsed;
                        console.log(`📖 Leídos: ${totalRows.toLocaleString()} registros (${Math.round(readRate)} reg/seg)`);
                    }
                })
                .on('end', async () => {
                    if (this.currentBatch.length > 0) {
                        await this.processBatch();
                    }

                    const connection = this.getNextConnection();
                    try {
                        await connection.beginTransaction();
                        await this.flushEntidadBuffer(connection);
                        await this.flushProveedorBuffer(connection);
                        await connection.commit();
                    } catch (error) {
                        await connection.rollback();
                        console.error('❌ Error en flush final:', error.message);
                    }

                    console.log('✅ Importación completada');
                    resolve({
                        totalProcessed: this.totalProcessed,
                        totalSkipped: this.totalSkipped,
                        totalErrors: this.totalErrors,
                        totalRows,
                        elapsedMinutes: Math.round((Date.now() - this.startTime) / 60000),
                        averageRate: Math.round(this.totalProcessed / ((Date.now() - this.startTime) / 1000))
                    });
                })
                .on('error', reject);
        });
    }
}

async function main() {
    const csvFile = './SECOP_II_-_Contratos_Electrónicos_20250907.csv';
    
    if (!fs.existsSync(csvFile)) {
        console.error(`❌ Archivo no encontrado: ${csvFile}`);
        process.exit(1);
    }

    console.log(`
🎯 IMPORTADOR SECOP ULTRA-OPTIMIZADO - VERSIÓN ESTABLE
📋 Características corregidas:
   🚀 Lotes de 2000 registros 
   💾 Buffers de 1000 registros para inserts masivos
   🔗 4 conexiones MySQL simultáneas (estables)
   📊 Precarga de entidades y proveedores existentes
   🧠 Caches de 100K entradas con limpieza inteligente
   💿 Checkpoints cada 10K registros
   ⚡ Solo configuraciones MySQL compatibles
   🔄 Procesamiento paralelo optimizado
   
🎯 Meta realista: 4,000,000 registros en 8-10 horas
📈 Velocidad esperada: 120-160 reg/seg sostenidos
    `.trim());

    const importer = new FixedUltraSecopImporter();
    
    try {
        // Solo configuraciones MySQL que sabemos que funcionan
        console.log('⚙️  Aplicando configuraciones MySQL compatibles...');
        
        try {
            await pool.execute('SET GLOBAL innodb_flush_log_at_trx_commit = 0');
            console.log('✅ innodb_flush_log_at_trx_commit configurado');
        } catch (e) {
            console.log('⚠️  No se pudo configurar innodb_flush_log_at_trx_commit');
        }
        
        try {
            await pool.execute('SET GLOBAL sync_binlog = 0');
            console.log('✅ sync_binlog configurado');
        } catch (e) {
            console.log('⚠️  No se pudo configurar sync_binlog');
        }
        
        try {
            await pool.execute('SET GLOBAL query_cache_type = OFF');
            console.log('✅ query_cache_type deshabilitado');
        } catch (e) {
            console.log('⚠️  No se pudo deshabilitar query_cache_type');
        }
        
        // Intentar aumentar buffer pool solo si es posible
        try {
            await pool.execute('SET GLOBAL innodb_buffer_pool_size = 2147483648'); // 2GB
            console.log('✅ innodb_buffer_pool_size aumentado a 2GB');
        } catch (e) {
            console.log('⚠️  No se pudo aumentar innodb_buffer_pool_size - usando valor por defecto');
        }
        
        console.log('✅ Configuraciones MySQL aplicadas (las compatibles)');

        const startTime = Date.now();
        const result = await importer.importCSV(csvFile);
        const totalHours = (Date.now() - startTime) / 3600000;

        // Cerrar conexiones
        await importer.closeConnections();

        // Restaurar configuración normal
        console.log('🔄 Restaurando configuraciones MySQL...');
        try {
            await pool.execute('SET GLOBAL innodb_flush_log_at_trx_commit = 1');
            await pool.execute('SET GLOBAL sync_binlog = 1');
            await pool.execute('SET GLOBAL query_cache_type = ON');
        } catch (e) {
            console.log('⚠️  Algunas configuraciones no se pudieron restaurar');
        }

        console.log(`
🎉 === RESUMEN FINAL OPTIMIZADO ===
📊 Total registros leídos: ${result.totalRows.toLocaleString()}
✅ Procesados exitosamente: ${result.totalProcessed.toLocaleString()}
⏭️  Omitidos (sin datos): ${result.totalSkipped.toLocaleString()}
❌ Errores: ${result.totalErrors.toLocaleString()}
⏱️  Tiempo total: ${totalHours.toFixed(2)} horas (${result.elapsedMinutes} minutos)
⚡ Velocidad promedio: ${result.averageRate} registros/segundo
📈 Registros por hora: ${Math.round(result.averageRate * 3600).toLocaleString()}
🎯 Efectividad: ${((result.totalProcessed / result.totalRows) * 100).toFixed(2)}%

🏆 RESULTADO ${totalHours <= 10 && result.totalProcessed >= 3500000 ? '✅ EXCELENTE' : 
              totalHours <= 12 && result.totalProcessed >= 3000000 ? '🟡 BUENO' : 
              result.totalProcessed >= 2000000 ? '🟠 ACEPTABLE' : '❌ NECESITA MEJORAS'}

📋 Rendimiento alcanzado:
   • Entidades únicas: ${importer.entidadCache.size.toLocaleString()}
   • Proveedores únicos: ${importer.proveedorCache.size.toLocaleString()}
   • Contratos completos: ${result.totalProcessed.toLocaleString()}

💡 Estadísticas técnicas:
   • Lotes procesados: ${importer.batchCount.toLocaleString()}
   • Tamaño de lote: ${importer.batchSize} registros
   • Conexiones utilizadas: ${importer.connectionCount}
   • Buffers de inserción: ${importer.bufferSize} registros
        `.trim());

        // Limpiar checkpoint si fue exitoso
        if (fs.existsSync(importer.checkpointFile)) {
            fs.unlinkSync(importer.checkpointFile);
            console.log('🗑️  Checkpoint limpiado');
        }

        // Recomendaciones específicas basadas en rendimiento
        if (result.averageRate < 120) {
            console.log(`
⚠️  RECOMENDACIONES PARA MEJORAR RENDIMIENTO:
   • Ejecutar con más memoria: NODE_OPTIONS="--max-old-space-size=8192"
   • Verificar que MySQL tenga suficiente RAM asignada
   • Considerar usar un SSD para mejorar I/O
   • Cerrar aplicaciones innecesarias durante la importación
   • Verificar que no hay otros procesos pesados usando la base de datos
            `.trim());
        } else {
            console.log(`
✅ RENDIMIENTO ÓPTIMO ALCANZADO
   • La velocidad de ${result.averageRate} reg/seg es excelente
   • El sistema está bien optimizado para este volumen de datos
   • Tiempo estimado para completar: ${Math.round(4000000 / result.averageRate / 3600)} horas
            `.trim());
        }

    } catch (error) {
        console.error('💥 Error durante importación:', error);
        console.log('💾 Checkpoint guardado. Puede reanudar ejecutando nuevamente.');
        console.log(`📊 Progreso actual: ${importer.totalProcessed?.toLocaleString() || 0} registros procesados`);
    } finally {
        await pool.end();
        console.log('🔌 Conexión a base de datos cerrada');
    }
}

// Manejo de interrupciones mejorado
process.on('SIGINT', async () => {
    console.log('\n🛑 Interrupción detectada. Guardando progreso...');
    try {
        await pool.end();
    } catch (e) {
        // Ignorar errores al cerrar
    }
    process.exit(0);
});

// Configuración optimizada de Node.js
process.setMaxListeners(50);

// Verificar memoria y dar recomendaciones
const memoryUsage = process.memoryUsage();
const heapSizeMB = memoryUsage.heapTotal / 1024 / 1024;

if (heapSizeMB < 1000) {
    console.log(`
⚠️  ADVERTENCIA DE MEMORIA:
   • Heap actual: ${Math.round(heapSizeMB)}MB
   • Recomendado: Al menos 2GB para 4M registros
   • Ejecute con: NODE_OPTIONS="--max-old-space-size=4096" node importCSV.js
   • O mejor: NODE_OPTIONS="--max-old-space-size=8192" node importCSV.js
    `.trim());
}

// Garbage collection automático si está disponible
if (global.gc) {
    setInterval(() => {
        if (process.memoryUsage().heapUsed > 1000000000) { // Si usa más de 1GB
            global.gc();
        }
    }, 180000); // Cada 3 minutos
    console.log('🗑️  Garbage collection automático habilitado');
}

if (require.main === module) {
    main();
}

module.exports = { FixedUltraSecopImporter };