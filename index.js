const fs = require('fs');
const parse = require('csv-parse');
const transform = require('stream-transform')
const stringify = require('csv-stringify')
const multistream = require('multistream')
const ipcidr = require('ip-cidr')
const R = require('ramda')

const inputs = [
    fs.createReadStream('test-ipv4.csv'),
    fs.createReadStream('test-ipv6.csv')
]
const geoLookup = fs.createReadStream('test-locations.csv')
const output = fs.createWriteStream('out.csv')
const parser = parse({})
const stringifier = stringify({});

const removeHeaders = transform((line, callback) => {
    if (line[0] !== 'network') {
        callback(null, line)
    }
})

// take a CIDR range and returns an array [startString, endString, startInt, endInt]
const convertCidrToRange = (cidr) => {
    const network = new ipcidr(cidr)
    const range = network.toRange()
    const start = network.start({type: 'bigInteger'}).toString()
    const end = network.end({type: 'bigInteger'}).toString()
    return R.insertAll(2, [start, end], range)
}

// transforms a incoming line by adding IP addresses and removes the CIDR range
const resolveIps = transform((line, callback) => {
    callback(null, R.pipe(
        R.insertAll(1, convertCidrToRange(line[0])),
        R.remove(0, 1)
    )(line))
})

// *TODO*
// const getGeoInfo = (geoid) => {

// }
// const resolveGeoNames = transform((line, callback) => {
//     console.log(`GEOID: ${line[1]}`)
//     callback(null, line)
// })

const loadGeoNames = new Promise((resolve, reject) => {
    let names = []
    const parser = parse({columns:true})
    geoLookup.pipe(parser)
    parser.on('readable', () => {
        let record
        while (record = parser.read()) {
            names.push({
                id: record.geoname_id, 
                name: record.country_name, 
                iso: record. country_iso_code
            })
        }
    })
    parser.on('error', (error) => reject(error))
    parser.on('end', () => resolve(names))
})

let geoNames = [];

(async () => {
    geoNames = await loadGeoNames
    // console.log(geoNames)

    await multistream(inputs)
        .pipe(parser)
        .pipe(removeHeaders)
        // .pipe(resolveGeoNames)
        .pipe(resolveIps)
        .pipe(stringifier)
        .pipe(output)

})()
