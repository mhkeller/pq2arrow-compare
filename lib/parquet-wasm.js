/**
 * Converts a parquet file on disk to an arrow file in memory
 * Adapted from the README example in https://github.com/kylebarron/parquet-wasm?tab=readme-ov-file
 *
 * @param {string} filepath
 * @returns {buffer}
 */
const arrow = require("apache-arrow");
const { parseRecordBatch } = require("arrow-js-ffi");
const { readFileSync } = require("fs");
const { readParquet, wasmMemory } = require("parquet-wasm");

module.exports = async function parquetToArrow(filepath) {
	// A reference to the WebAssembly memory object.
	const WASM_MEMORY = wasmMemory();

	const buf = readFileSync(filepath);
	const parquetUint8Array = new Uint8Array(Buffer.from(buf));

	const wasmArrowTable = readParquet(parquetUint8Array).intoFFI();

	const recordBatches = [];
	for (let i = 0; i < wasmArrowTable.numBatches(); i++) {
		// Note: Unless you know what you're doing, setting `true` below is recommended to _copy_
		// table data from WebAssembly into JavaScript memory. This may become the default in the
		// future.
		const recordBatch = parseRecordBatch(
			WASM_MEMORY.buffer,
			wasmArrowTable.arrayAddr(i),
			wasmArrowTable.schemaAddr(),
			true
		);
		recordBatches.push(recordBatch);
	}

	const table = new arrow.Table(recordBatches);

  // Skip this step converting it to bytes if you just want the table
	const ipcStream = arrow.tableToIPC(table, 'stream');
	const bytes = Buffer.from(ipcStream, 'utf-8');

	// VERY IMPORTANT! You must call `drop` on the Wasm table object when you're done using it
	// to release the Wasm memory.
	// Note that any access to the pointers in this table is undefined behavior after this call.
	// Calling any `wasmArrowTable` method will error.
	wasmArrowTable.drop();

	return bytes;
}
