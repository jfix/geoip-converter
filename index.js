const fs = require('fs')
const parse = require('csv-parse')
const path = require('path')
const transform = require('stream-transform')
const stringify = require('csv-stringify')
const multistream = require('multistream')
const ipcidr = require('ip-cidr')
const R = require('ramda')
const argv = require('minimist')(process.argv.slice(2))

let resolved = 0
let unresolved = 0

// handle arguments
let indir
let outdir

const handleArguments = () => {
    if (!argv.indir && !argv.indir) {
        indir = './data'
        outdir = './data'
    }
    if (argv.indir && !argv.outdir) outdir = argv.indir
    if (!argv.indir && argv.outdir) {
        indir = './data'
        outdir = argv.outdir
    }
    if (argv.outdir) outdir = argv.outdir
    if (argv.indir) indir = argv.indir
    console.dir(`INDIR: ${path.resolve(indir)} -- OUTDIR: ${path.resolve(outdir)}`);
}
handleArguments()

const ipv4rs = fs.createReadStream(path.resolve(indir, 'GeoLite2-Country-Blocks-IPv4.csv'))
ipv4rs.on('error', (err) => { console.error(err.message); process.exit(1) })

const ipv6rs = fs.createReadStream(path.resolve(indir, 'GeoLite2-Country-Blocks-IPv6.csv'))
ipv6rs.on('error', (err) => { console.error(err.message); process.exit(1) })

const geoLookup = fs.createReadStream(path.resolve(indir, 'GeoLite2-Country-Locations-en.csv'))
geoLookup.on('error', (err) => { console.error(err.message); process.exit(1) })

const output = fs.createWriteStream(path.resolve(outdir, 'GeoIPCountryWhois.csv'))
output.on('error', (err) => { console.error(err.message); process.exit(1) })

const inputs = [ ipv4rs, ipv6rs]
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
        resolved++
        return [info.iso, info.name]
    }
    unresolved++
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
                console.log(`INFO: ${resolved} resolved, ${unresolved} unresolved (about ${ Math.round( (unresolved * 100 / resolved) * 100 + Number.EPSILON ) / 100 }%).`)
                resolve()
            })
    })
})()
