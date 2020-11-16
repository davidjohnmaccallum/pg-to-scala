#!/usr/bin/env node

const { Client } = require('pg')
require('dotenv').config()

const [,,schemaName,tableName] = process.argv

const user = process.env.DB_USER
const host = process.env.DB_HOST
const database = process.env.DB_NAME
const password = process.env.DB_PASSWORD
const port = process.env.DB_PORT

if (!user) return console.log("Set DB_USER environment variable.")
if (!host) return console.log("Set DB_HOST environment variable.")
if (!database) return console.log("Set DB_NAME environment variable.")
if (!password) return console.log("Set DB_PASSWORD environment variable.")
if (!schemaName || !tableName) return console.log("Usage: pg-to-scala schemaName tableName")

go().then(console.log).catch(console.error)

async function go() {

  let client
  
  try {

    client = new Client({ user, host, database, password, port })
    client.connect()

    const sql = `
    select column_name, udt_name, is_nullable, column_default, character_maximum_length
    from information_schema.columns 
    where table_schema = '${schemaName}'
      and table_name = '${tableName}';`

    const res = await client.query(sql)

    const fields = res.rows.map(it => {
      const parts = [
        toCamelCase(it.column_name)+":",
        mapType(it.udt_name, it.is_nullable === 'YES', it.column_default)+",",
        "//",
        it.column_name,
        it.udt_name,   
        `is_nullable:${it.is_nullable}`, 
      ]
      if (it.column_default) parts.push(`column_default:${it.column_default}`)
      if (it.character_maximum_length) parts.push(`character_maximum_length:${it.character_maximum_length}`)
      return parts.join(" ")
    })
    
    return `
case class ${capitalizeFirstLetter(toCamelCase(tableName))} (
${fields.map(it=>"\t"+it).join("\n")}
)
`

  } finally {
    if (client) client.end().catch(console.error)
  }

}

function mapType(udtName, isNullable) {
  const mapping = {
    "varchar": "String",
    "_varchar": "String",
    "text": "String",
    "json": "String",
    "timestamp": "DateTime",
    "date": "DateTime",
    "bpchar": "Boolean",
    "numeric": "Double",
    "int4": "Int",
    "int8": "Long",
    "bool": "Boolean"
  }

  const mappedType = mapping[udtName] ? mapping[udtName] : capitalizeFirstLetter(toCamelCase(udtName))
  if (!mapping[udtName]) console.error("Unknown mapping", udtName)

  if (!isNullable) return mappedType
  return `Option[${mappedType}]`
}

function toCamelCase(string) {
  return string.replace(/_(.)/g, (_, group)=>group.toUpperCase())
}

function capitalizeFirstLetter(string) {
  return string.charAt(0).toUpperCase() + string.slice(1);
}