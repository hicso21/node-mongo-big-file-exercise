const Records = require("./records.model");
const fs = require("fs");
const csv = require("csv-parser");

const upload = async (req, res) => {
  const { file } = req;

  if (!file)
    return res.status(400).json({
      error: "No se recibió ningún archivo",
    });

  if (file.mimetype !== "text/csv" && !file.originalname.endsWith(".csv"))
    return res.status(400).json({
      error: "El archivo debe ser un CSV",
    });

  let processedRecords = 0;
  let errorRecords = 0;
  const batchSize = 10000;
  let batch = [];

  const processBatch = async (records) => {
    try {
      // Filtrado
      const validRecords = records.filter((record) => record !== null);

      if (validRecords.length === 0) return 0;

      await Records.insertMany(validRecords, {
        writeConcern: { w: 1, j: false },
      });

      return validRecords.length;
    } catch (error) {
      // Si hay duplicados, contar solo los insertados exitosamente
      if (error.code === 11000 && error.result && error.result.result)
        return error.result.result.nInserted || 0;

      console.error("❌ Error al procesar lote:", error.message);
      return 0;
    }
  };

  const validateAndTransformRecord = (record) => {
    if (!record?.id || !record?.email) return null;

    return {
      id: record.id,
      firstname: record.firstname || "",
      lastname: record.lastname || "",
      email: record.email,
      email2: record.email2 || "",
      profession: record.profession || "",
      createdAt: new Date(),
    };
  };

  try {
    const startTime = Date.now();
    let recordCount = 0;

    const csvStream = fs
      .createReadStream(file.path, {
        highWaterMark: 64 * 1024,
      })
      .pipe(
        csv({
          separator: ",",
          skipEmptyLines: true,
          skipLinesWithError: true,
          maxRowBytes: 1024 * 1024,
        })
      );

    csvStream.on("data", (record) => {
      recordCount++;

      const transformedRecord = validateAndTransformRecord(record);

      if (!transformedRecord) {
        errorRecords++;
        return;
      }

      batch.push(transformedRecord);

      if (batch.length >= batchSize) {
        processBatch([...batch])
          .then((processed) => {
            processedRecords += processed;
          })
          .catch((err) => {
            console.error("Error en batch async:", err);
          });

        batch = [];
      }
    });

    // Esperar a que termine la lectura
    await new Promise((resolve, reject) => {
      csvStream.on("end", resolve);
      csvStream.on("error", reject);
    });

    // Procesar último batch si queda algo
    if (batch.length > 0) {
      const processed = await processBatch(batch);
      processedRecords += processed;
    }

    // Espera de los batches async
    await new Promise((resolve) => setTimeout(resolve, 2000));

    const endTime = Date.now();
    const processingTime = (endTime - startTime) / 1000;

    return res.status(200).json({
      message: "Archivo procesado exitosamente",
      stats: {
        totalRecordsRead: recordCount,
        processedRecords,
        errorRecords,
        processingTime: `${processingTime}s`,
        recordsPerSecond: Math.round(processedRecords / processingTime),
      },
    });
  } catch (error) {
    return res.status(500).json({
      error: "Error interno del servidor al procesar el archivo",
      details: error.message,
      stats: {
        processedRecords,
        errorRecords,
      },
    });
  } finally {
    if (fs.existsSync(file.path)) {
      fs.unlinkSync(file.path);
    }
  }
};

const list = async (_, res) => {
  try {
    const data = await Records.find({}).limit(10).lean();
    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json(err);
  }
};

module.exports = {
  upload,
  list,
};
