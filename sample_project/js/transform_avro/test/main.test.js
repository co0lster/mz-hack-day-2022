const transform = require("../src/main");
const { createRecordBatch } = require("@vectorizedio/wasm-api");
const assert = require("assert");

const record = {
"icao24": "a09a01",
"callsign": "N138ME",
"origin_country": "United States",
"time_position": "1645129232",
"last_contact": "1645129232",
"longitude": "-121.8938",
"latitude": "38.0786",
"baro_altitude": "1082.04",
"on_ground": "false",
"velocity": "43.98",
"true_track": "114.17",
"vertical_rate": "0.33'",
"sensors": "null",
"geo_altitude": "1158.24",
"squawk": "5221",
"spi": "false", 
"position_source": "0", 
}

const record2 = {
  "Date":"12/10/2021",
  "CloseLast":"4712.02",
  "Volume":"--",
  "Open":"4687.64",
  "High":"4713.57",
  "Low":"4670.24"
};
const recordBatch = createRecordBatch({
  records: [{value: JSON.stringify(record)}]
});

/* Mocha test transform json to avro */
describe("transform", function() {
  it("transforms json to avro", function() {
    return transform.default.apply(recordBatch).then(result => {
      assert.equal(result.size, 1);
      assert(result.get("result"));
      result.get("result").records.forEach(avroRecord => {
        obj = transform.schema.fromBuffer(avroRecord.value);
        assert.equal(
          JSON.stringify(obj),
          JSON.stringify(record)
        );
      })
    });
  });
});
