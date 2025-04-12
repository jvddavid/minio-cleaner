import { createWriteStream } from 'fs'
import { mkdir } from 'fs/promises'
import { Client } from 'minio'
import { dirname } from 'path'
import type { ObjectInfo } from './types'

const client = new Client({
  endPoint: process.env.MINIO_ENDPOINT || 'localhost',
  useSSL: true,
  region: 'sa-east-1',
  accessKey: process.env.MINIO_ACCESS_KEY,
  secretKey: process.env.MINIO_SECRET_KEY
})

const bucketName = 'public'
const folder = process.env.MINIO_FOLDER || 'verification/broker'
const folderName = folder
const savePath = 'downloads/' + folder.split('/').pop() + '/'

async function objDownload(obj: ObjectInfo) {
  if (obj.name == null) return
  const fullPath = `${savePath}${obj.name.replace(folderName, '')}`
  const dir = dirname(fullPath)
  try {
    const stat = await mkdir(dir, { recursive: true })
    if (stat) {
      console.log(`Created directory: ${dir}`)
    }
  } catch (err: any) {
    if (err.code !== 'EEXIST') {
      console.error(`Error creating directory: ${dir}`, err)
    }
  }
  const fileStream = await client.getObject(bucketName, obj.name)
  const write = createWriteStream(fullPath, {
    flags: 'w',
    encoding: 'binary'
  })
  fileStream.pipe(write)
  await new Promise<void>((resolve, reject) => {
    write.on('finish', resolve)
    write.on('error', reject)
  })
}

async function objDelete(obj: ObjectInfo) {
  if (obj.name == null) return
  await client.removeObject(bucketName, obj.name)
}

async function main() {
  const files = await client.listObjects(bucketName, folderName, true)
  console.log('Fetching file list...')
  let filesDownloaded = 0
  let count = 0
  const promises: Promise<void>[] = []
  await new Promise(resolve => {
    files.on('data', obj => {
      count++
      if (count % 100 === 0) {
        console.log(`Fetched ${count} files...`)
      }
      const promise = objDownload(obj)
          .then(() => {
            filesDownloaded++
            if (filesDownloaded % 100 === 0) {
              console.log(`Downloaded ${filesDownloaded} files...`)
            }
            return objDelete(obj)
              .then(() => {
                console.log(`Deleted ${obj.name}`)
              })
              .catch(err => {
                console.error(`Error deleting ${obj.name}:`, err)
              })
          })
          .catch(err => {
            console.error('Error downloading file:', err)
          }).finally(() => {
            promises.splice(promises.indexOf(promise), 1)
          })
      promises.push(
        promise
      )
    })
    files.on('end', () => {
      resolve(null)
    })
  })
  await Promise.all(promises)
  console.log(`Downloaded ${filesDownloaded} of ${count} files`)
  console.log('All files downloaded')
  console.log('Done')
}

main()
  .then(() => {
    console.log('done')
  })
  .catch(err => {
    console.error(err)
    process.exit(1)
  })
