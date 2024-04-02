const { execSync } = require('child_process');

async function run () {
	console.time('Starting rust conversion');
	const result = execSync('./lib/parquet2arrow --input ./data/yellow_tripdata_2024-01.parquet --output ./out/yellow_tripdata_2024-01.arrow');
	console.timeEnd('Starting rust conversion');
	console.log(result);
}

run();
