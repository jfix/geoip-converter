const fs = require('fs')
const parse = require('csv-parse')
const transform = require('stream-transform')
const stringify = require('csv-stringify')
const multistream = require('multistream')
const ipcidr = require('ip-cidr')
const R = require('ramda')

const inputs = [
    fs.createReadStream('data/GeoLite2-Country-Blocks-IPv4.csv'),
    fs.createReadStream('data/GeoLite2-Country-Blocks-IPv6.csv')
]
const geoLookup = fs.createReadStream('data/GeoLite2-Country-Locations-en.csv')
const output = fs.createWriteStream('data/GeoIPCountryWhois.csv')
const parser = parse({})
const stringifier = stringify({quoted: true})

const removeHeaders = transform((line) => {
    if (line[0] !== 'network') return line
})

// take a CIDR range and return an array [startString, endString, startInt, endInt]
const convertCidrToRange = (cidr) => {
    const network = new ipcidr(cidr)
    const range = network.toRange()
    const start = network.start({type: 'bigInteger'}).toString()
    const end = network.end({type: 'bigInteger'}).toString()
    return range.concat([start, end])
}

// transforms a incoming line by adding IP addresses and removes the CIDR range
const resolveIps = transform((line) => R.pipe(R.insertAll(1, convertCidrToRange(line[0])))(line))

const getGeoInfo = (geoid) => {
    const info = geoNames.get(geoid);
    if (info) {
        return [info.iso, info.name]
    }
    return ['ZZ', `Unknown ${geoid}`]
}

const resolveGeoNames = transform((line) => {
    const arr = getGeoInfo(line[5])
    const newLine = R.insertAll(5, arr)(line)
    return newLine
})

const cleanUpCells = transform((line) => {
    const newLine = R.pipe(
        R.remove(0, 1),
        R.remove(6, 5)
    )(line)
    return newLine
})

let geoNames = null;

(async () => {
    geoNames = await new Promise((resolve, reject) => {
        console.time('loadGeoNames')
        console.log("> loadGeoNames")
        let names = new Map()
        const parser = parse({columns:true})
        geoLookup.pipe(parser)
        parser.on('readable', () => {
            let record
            while (record = parser.read()) {
                names.set(record.geoname_id, {
                    id: record.geoname_id, 
                    name: record.country_name, 
                    iso: record. country_iso_code
                })
            }
        })
        parser.on('error', (error) => reject(error))
        parser.on('end', () => {
            console.log(`INFO: loaded ${names.size} geonames.`)
            console.timeEnd('loadGeoNames')
            return resolve(names)
        })
    })

    await new Promise((resolve, reject) => {
        console.time('doTheWork')
        console.log("> doTheWork")
        
        multistream(inputs)
            .pipe(parser)
            .pipe(removeHeaders)
            .pipe(resolveIps)
            .pipe(resolveGeoNames)
            .pipe(cleanUpCells)
            .pipe(stringifier)
            .pipe(output)
            .on('close', () => {
                console.timeEnd('doTheWork')
                resolve()
            })
    })
})()
