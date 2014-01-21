var RedshiftCopyS3 = require('../lib/RedshiftCopyS3.js');
var async = require('async');
var path = require('path');
var s3shield = require('s3shield');
var assert = require('assert');
var knox = require('knox');
var dbStuff = require('db-stuff');
var rc = require('rc');

var clientProviderClass = s3shield.S3ClientProviderSelector.get('knox');
var clientProvider = new clientProviderClass();

var config = rc('test');

var testFunction = describe;
var s3Client;

if (!config.aws) {
	testFunction = describe.skip;
	console.log('skipping this test since no credentials were supplied (.testrc)');
} else {
	s3Client = knox.createClient({
	    key: 		config.aws.accessKeyId,
	  	secret: 	config.aws.secretAccessKey,
	  	bucket: 	config.aws.bucket,
	  	region: 	config.aws.region,
	  	endpoint: 	config.aws.endpoint
	});
}

var optionsMock = config.redshiftOptions;
var rowsMock = [ [1000, '127.0.0.1', 'mockedId'], [2000, '127.0.0.2', 'mockedId2'] ];
var testFile = '1000|127.0.0.1|mockedId\n2000|127.0.0.2|mockedId2\n';

var selectQuery = 'SELECT * FROM ' + config.redshiftOptions.tableName + ' ORDER BY ip ASC;';
var truncateQuery = 'TRUNCATE TABLE ' + config.redshiftOptions.tableName + ' ;';

testFunction('RedshiftCopyS3', function(){
	it('should upload file to s3 and load data to datastore', function(done){
		this.timeout(180000);

		function testDone(err, services) {
			if (err) {
				console.error(err);
				done(err);
			} else {
				console.log('All done!');
				done(null);
			}
		}

		async.waterfall([
			initTest,
			truncateTestTable,
			doCopy,
			verifyUpload,
			verifyInsertedData
		], testDone);
	});
});

function initTest(callback) {
	var services = {};
	var ds = dbStuff.create(config.redshift, function(){
		services.ds = ds;
		callback(null, services);
	});
}

function truncateTestTable(services, callback) {
	console.log('Truncating test table...');
	services.ds.query(truncateQuery, function(err, result){
		callback(err, services);
	});
}

function doCopy(services, callback) {
	console.log('Doing copy...');

	var rsbl = new RedshiftCopyS3(services.ds, clientProvider, optionsMock, config.aws);
	services.rsbl = rsbl;

	rsbl.on('flush', function(flushOp){
		console.log('Flushing...');
		flushOp.on('success', function(flushOp){
			console.log('Flushed.');
			services.flushOp = flushOp;
			callback(null, services);
		});
		flushOp.on('error', function(err){
			console.error(err);
			callback(err, services);
		});
	});

	rsbl.insert(rowsMock[0]);
	rsbl.insert(rowsMock[1]);
}

/*
	download the file from s3 and compare to test file data
*/
function verifyUpload(services, callback) {
	console.log('Verifying file in s3 bucket...');
	s3Client.getFile(services.flushOp.key, function(err, res) {

		services.downloadedFile = '';

		function readResponse() {

			if (!res) return;

			function readMore() {
				var result = res.read();

				if (result) {
					services.downloadedFile += result;
					readMore();
				}
			}

			res.on('readable', readMore);

			res.on('end', function () {
				assert.strictEqual(services.downloadedFile, testFile);
				callback(null, services);
			});
		}

		readResponse();
	});
}

function verifyInsertedData(services, callback) {
	console.log('Verifying data in datastore...')
	services.ds.query(selectQuery, function(err, result) {
		if (err) {
			callback(err);
			return;
		}
		assert.equal(result.rows[0]['ip'], '127.0.0.1');
		assert.equal(result.rows[1]['ip'], '127.0.0.2');

		callback(null);
	});
}