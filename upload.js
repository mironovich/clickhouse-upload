const { ClickHouse } = require('clickhouse')
const readline = require('readline')
const fs = require('fs')
const util = require('util')
require('isomorphic-fetch')

const CONFIG = JSON.parse(fs.readFileSync('./.config'))
const LOCAL = true

LOCAL && (CONFIG.CLICKHOUSE.url = CONFIG.CLICKHOUSE.local)

const clickhouse = new ClickHouse(CONFIG.CLICKHOUSE)

const query = {
  create: `
    create table ${CONFIG.DB}.${CONFIG.TABLE}
    (
      ID_CONTACT UInt32,
      EXTERNAL_ID UInt32,
      CAMPAIGN_SOURCE Nullable(String),
      CAMPAIGN Nullable(String),
      CAMPAIGN_MEDIUM Nullable(String),
      CAMPAIGN_TERM Nullable(String),
      CAMPAIGN_CONTENT Nullable(String),
      DWELLING_REGION Nullable(String),
      AGE Nullable(String),
      DT_APPLICATION_START DateTime,
      PRODUCT Nullable(String),
      ASSIST_METHOD Nullable(String),
      ONLINE_DECISION Nullable(String),
      AUX_ID_1 Nullable(String),
      AUX_ID_2 Nullable(String),
      AUX_ID_3 Nullable(String),
      PARTNER_EXTERNAL_ID Nullable(String),
      contacted Nullable(String),
      APPLICATION_DT Nullable(DateTime),
      applied Nullable(String),
      issued Nullable(String),
      LOAN_AMOUNT Nullable(String),
      ACTIVATED Nullable(String),
      ACTIVATION_DATE Nullable(DateTime),
      UNIQ Nullable(String)
    ) engine = ReplacingMergeTree()
    order by ID_CONTACT
    partition by toYYYYMM(DT_APPLICATION_START)`,
  drop: `
    drop table ${CONFIG.DB}.${CONFIG.TABLE}`,
  write: `
    insert into ${CONFIG.DB}.${CONFIG.TABLE}
      (ID_CONTACT)
    values (123)`,
  read: `
      select *
      from ${CONFIG.DB}.${CONFIG.TABLE}
  `,
  insert: `
    insert into ${CONFIG.DB}.${CONFIG.TABLE}
    format JSONEachRow
    `,
  optimize: `
    optimize table ${CONFIG.DB}.${CONFIG.TABLE}
  `,
}

const logError = message => {
  console.log(message)
  message = util.format(message)
  message = '\n' + new Date().toString() + '\n' + message + '\n'
  fs.appendFile(CONFIG.LOGS, message, function(err) {
    if (err) {
      return console.log(err)
    }
    console.log('Logs were saved')
  })
}

const printProgress = (message, progress) => {
  readline.clearLine(process.stdout)
  readline.cursorTo(process.stdout, 0)
  process.stdout.write(`${message} ... ${progress}%`)
}

const sendData = async data => {
  const l = data.length
  for (i = 0, j = data.length; i < j; i += CONFIG.ROW_LIMIT) {
    let chunk = data.slice(i, i + CONFIG.ROW_LIMIT)
    let req = chunk.reduce(
      (a, cv) => a + JSON.stringify(cv) + '\n',
      query.insert
    )
    // console.log(req)
    const res = await clickhouse.query(req).toPromise()
    printProgress('Sending', (((i + CONFIG.ROW_LIMIT) / l) * 100).toFixed(2))
  }
}

const sendUrl = () => {
  fetch(CONFIG.URL)
    .then(res => res.json())
    .then(json => {
      console.log('Rows: ', json.data.length)
      // const data = json.data.slice(0, 2)

      sendData(json.data)
        .then(() => clickhouse.query(query.optimize).toPromise())
        .then(r => {
          r.r == 1 && console.log('\nFinished')
        })
        .catch(error => logError(error))
    })
}

const sendFile = () => {
  const data = require('./results/test.json').data
  sendData(data)
    .then(() => clickhouse.query(query.optimize).toPromise())
    .then(r => {
      r.r == 1 && console.log('\nFinished')
    })
    .catch(error => logError(error))
}

const dropTable = () => {
  clickhouse
    .query(query.drop)
    .toPromise()
    .then(() => clickhouse.query(query.create).toPromise())
    .then(r => {
      r.r == 1 && console.log('\nFinished')
    })
    .catch(error => logError(error))
}

const createTable = () => {
  clickhouse
    .query(query.create)
    .toPromise()
    .then(r => {
      r.r == 1 && console.log('\nFinished')
    })
    .catch(error => logError(error))
}

sendUrl()
