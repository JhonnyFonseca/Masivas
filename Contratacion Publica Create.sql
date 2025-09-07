/* ============================
   1) Crear base de datos
============================ */

USE secop_contratos;

/* ============================
   3) Tablas maestras
============================ */

CREATE TABLE Entidad (
    EntidadId               INT AUTO_INCREMENT PRIMARY KEY,
    nombre_entidad          VARCHAR(300) NOT NULL,
    nit_entidad             VARCHAR(20)  NOT NULL,
    departamento            VARCHAR(120) NULL,
    ciudad                  VARCHAR(120) NULL,
    localizacion            VARCHAR(400) NULL,
    orden                   VARCHAR(120) NULL,
    sector                  VARCHAR(120) NULL,
    rama                    VARCHAR(120) NULL,
    entidad_centralizada    BOOLEAN NOT NULL DEFAULT FALSE,
    codigo_entidad          INT NULL,
    CONSTRAINT UQ_Entidad_NIT UNIQUE (nit_entidad),
    CONSTRAINT UQ_Entidad_Codigo UNIQUE (codigo_entidad)
);

CREATE TABLE RepresentanteLegal (
    RepId                               INT AUTO_INCREMENT PRIMARY KEY,
    EntidadId                           INT NOT NULL,
    nombre_representante_legal          VARCHAR(200) NULL,
    nacionalidad_representante_legal    VARCHAR(120) NULL,
    domicilio_representante_legal       VARCHAR(250) NULL,
    tipo_de_identificacion_representante_legal VARCHAR(50) NULL,
    identificacion_representante_legal  VARCHAR(50) NULL,
    genero_representante_legal          VARCHAR(50) NULL,
    FOREIGN KEY (EntidadId) REFERENCES Entidad(EntidadId)
);

CREATE TABLE Proveedor (
    ProveedorId         INT AUTO_INCREMENT PRIMARY KEY,
    codigo_proveedor    VARCHAR(50) NULL,
    tipodocproveedor    VARCHAR(100) NULL,
    documento_proveedor VARCHAR(30) NULL,
    proveedor_adjudicado VARCHAR(300) NULL,
    es_grupo            BOOLEAN NOT NULL DEFAULT FALSE,
    es_pyme             BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT UQ_Proveedor_Codigo UNIQUE (codigo_proveedor)
);

/* ============================
   4) Núcleo: CONTRATO
============================ */
CREATE TABLE Contrato (
    ContratoId                      INT AUTO_INCREMENT PRIMARY KEY,
    EntidadId                       INT NOT NULL,
    ProveedorId                     INT NULL,
    proceso_de_compra               VARCHAR(80)  NULL,
    id_contrato                     VARCHAR(80)  NULL,
    referencia_del_contrato         VARCHAR(120) NULL,
    estado_contrato                 VARCHAR(80)  NULL,
    codigo_de_categoria_principal   VARCHAR(32)  NULL,
    descripcion_del_proceso         VARCHAR(1000) NULL,
    tipo_de_contrato                VARCHAR(120) NULL,
    modalidad_de_contratacion       VARCHAR(200) NULL,
    justificacion_modalidad_de      VARCHAR(1000) NULL,
    fecha_de_firma                  DATETIME  NULL,
    fecha_de_inicio_del_contrato    DATETIME  NULL,
    fecha_de_fin_del_contrato       DATETIME  NULL,
    fecha_de_inicio_de_ejecucion    DATETIME  NULL,
    fecha_de_fin_de_ejecucion       DATETIME  NULL,
    condiciones_de_entrega          VARCHAR(500) NULL,
    habilita_pago_adelantado        BOOLEAN NOT NULL DEFAULT FALSE,
    liquidacion                     BOOLEAN NOT NULL DEFAULT FALSE,
    obligacion_ambiental            BOOLEAN NOT NULL DEFAULT FALSE,
    obligaciones_postconsumo        BOOLEAN NOT NULL DEFAULT FALSE,
    reversion                       BOOLEAN NOT NULL DEFAULT FALSE,
    origen_de_los_recursos          VARCHAR(200) NULL,
    destino_gasto                   VARCHAR(200) NULL,
    estado_bpin                     VARCHAR(80)  NULL,
    codigo_bpin                     VARCHAR(50)  NULL,
    anno_bpin                       VARCHAR(10)  NULL,
    espostconflicto                 BOOLEAN NOT NULL DEFAULT FALSE,
    dias_adicionados                INT NULL,
    puntos_del_acuerdo              VARCHAR(300) NULL,
    pilares_del_acuerdo             VARCHAR(300) NULL,
    urlproceso                      VARCHAR(500) NULL,
    ultima_actualizacion            DATETIME  NULL,
    fecha_inicio_liquidacion        DATETIME  NULL,
    fecha_fin_liquidacion           DATETIME  NULL,
    objeto_del_contrato             VARCHAR(1000) NULL,
    duracion_del_contrato           VARCHAR(200)  NULL,
    el_contrato_puede_ser_prorrogado BOOLEAN NOT NULL DEFAULT FALSE,
    fecha_de_notificacion_de_prorrogacion DATETIME NULL,

    CONSTRAINT UQ_Contrato_IdPlataforma UNIQUE (id_contrato),
    FOREIGN KEY (EntidadId) REFERENCES Entidad(EntidadId),
    FOREIGN KEY (ProveedorId) REFERENCES Proveedor(ProveedorId),
    INDEX IX_Contrato_Entidad (EntidadId),
    INDEX IX_Contrato_Proveedor (ProveedorId),
    INDEX IX_Contrato_ProcCompra (proceso_de_compra)
);

/* ============================
   5) Finanzas del contrato
============================ */
CREATE TABLE ContratoFinanzas (
    ContratoId                          INT PRIMARY KEY,
    valor_del_contrato                  DECIMAL(18,2) NULL,
    valor_de_pago_adelantado            DECIMAL(18,2) NULL,
    valor_facturado                     DECIMAL(18,2) NULL,
    valor_pendiente_de_pago             DECIMAL(18,2) NULL,
    valor_pagado                        DECIMAL(18,2) NULL,
    valor_amortizado                    DECIMAL(18,2) NULL,
    valor_pendiente_de_amortizacion     DECIMAL(18,2) NULL,
    valor_pendiente_de_ejecucion        DECIMAL(18,2) NULL,
    saldo_cdp                           DECIMAL(18,2) NULL,
    saldo_vigencia                      DECIMAL(18,2) NULL,
    FOREIGN KEY (ContratoId) REFERENCES Contrato(ContratoId)
);

/* ============================
   6) Desagregación de recursos
============================ */
CREATE TABLE ContratoRecursos (
    ContratoId                                       INT PRIMARY KEY,
    presupuesto_general_de_la_nacion_pgn             DECIMAL(18,2) NULL,
    sistema_general_de_participaciones               DECIMAL(18,2) NULL,
    sistema_general_de_regalias                      DECIMAL(18,2) NULL,
    recursos_propios_alcaldias_gobernaciones_resguardos DECIMAL(18,2) NULL,
    recursos_de_credito                              DECIMAL(18,2) NULL,
    recursos_propios                                 DECIMAL(18,2) NULL,
    FOREIGN KEY (ContratoId) REFERENCES Contrato(ContratoId)
);

/* ============================
   7) Datos bancarios del contrato
============================ */
CREATE TABLE ContratoBancario (
    ContratoId          INT PRIMARY KEY,
    nombre_del_banco    VARCHAR(200) NULL,
    tipo_de_cuenta      VARCHAR(50)  NULL,
    numero_de_cuenta    VARCHAR(50)  NULL,
    FOREIGN KEY (ContratoId) REFERENCES Contrato(ContratoId)
);

/* ============================
   8) Responsables del contrato
============================ */
CREATE TABLE ContratoResponsable (
    ResponsableId       INT AUTO_INCREMENT PRIMARY KEY,
    ContratoId          INT NOT NULL,
    rol                 VARCHAR(40) NOT NULL,  -- 'Supervisor' | 'OrdenadorGasto' | 'OrdenadorPago'
    nombre              VARCHAR(200) NULL,
    tipo_documento      VARCHAR(40)  NULL,
    numero_documento    VARCHAR(40)  NULL,
    FOREIGN KEY (ContratoId) REFERENCES Contrato(ContratoId),
    CONSTRAINT CK_ContratoResponsable_Rol CHECK (rol IN ('Supervisor', 'OrdenadorGasto', 'OrdenadorPago')),
    INDEX IX_Responsable_ContratoRol (ContratoId, rol)
);