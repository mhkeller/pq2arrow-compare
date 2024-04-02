const parquetWasm = require('./lib/parquet-wasm.js');

async function run () {
	console.time('Starting parquet-wasm conversion');
	const result = await parquetWasm('./data/yellow_tripdata_2024-01.parquet');
	console.timeEnd('Starting parquet-wasm conversion');
	console.log(result);
}

run();
