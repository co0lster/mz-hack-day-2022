const avro = require("avro-js");
const {
  SimpleTransform,
  PolicyError,
  PolicyInjection,
  calculateRecordBatchSize
} = require("@vectorizedio/wasm-api");

const transform = new SimpleTransform();

/**
 * Topics that fire the transform function
 * - Earliest
 * - Stored
 * - Latest
 */
transform.subscribe([["flight_information", PolicyInjection.Latest]]);

/**
 * The strategy the transform engine will use when handling errors
 * - SkipOnFailure
 * - Deregister
 */
transform.errorHandler(PolicyError.SkipOnFailure);

/* TODO: Fetch Avro schema from repository */
const schema = avro.parse({
  name: "flight_information",
  type: "record",
  fields: [
    {name: "icao24", type: "string"},
    {name: "callsign", type: "string"},
    {name: "origin_country", type: "string"},
    {name: "time_position", type: "string"},
    {name: "last_contact", type: "string"},
    {name: "longitude", type: "string"},
    {name: "latitude", type: "string"},
    {name: "baro_altitude", type: "string"},
    {name: "on_ground", type: "string"},
    {name: "velocity", type: "string"},
    {name: "true_track", type: "string"},
    {name: "vertical_rate", type: "string"},
    {name: "sensors", type: "string"},
    {name: "geo_altitude", type: "string"},
    {name: "squawk", type: "string"},
    {name: "spi", type: "string"},
    {name: "position_source", type: "string"}
    ]
});




/* Auxiliar transform function for records */
const toAvro = (record) => {  
  const obj = JSON.parse(record.value);
  const newRecord = {
    ...record,
    value: schema.toBuffer(obj),
  };
  return newRecord;  
}

/* Transform function */
transform.processRecord((recordBatch) => {
  const result = new Map();
  const transformedRecord = recordBatch.map(({ header, records }) => {
    return {
      header,
      records: records.map(toAvro),
    };
  });
  result.set("result", transformedRecord);
  // processRecord function returns a Promise
  return Promise.resolve(result);
});

exports["default"] = transform;
exports["schema"] = schema;
