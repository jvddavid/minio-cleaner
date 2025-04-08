import { createWriteStream } from 'fs'
import { mkdir } from 'fs/promises'
import { Client } from 'minio'
import {
  dirname
} from 'path'
import type { ObjectInfo } from './types'

const client = new Client({
  endPoint: process.env.MINIO_ENDPOINT || 'localhost',
  useSSL: true,
  region: 'sa-east-1',
  accessKey: process.env.MINIO_ACCESS_KEY,
  secretKey: process.env.MINIO_SECRET_KEY
})

async function main() {
  const bucketName = 'public'
  const folder = process.env.MINIO_FOLDER || 'verification/broker'
  const folderName = folder
  const savePath = 'downloads/' + folder.split('/').pop() + '/'

  const files = await client.listObjects(bucketName, folderName, true)
  const fileList: ObjectInfo[] = []
  console.log('Fetching file list...')
  await new Promise(resolve => {
    let count = 0
    files.on('data', obj => {
      fileList.push(obj)
      count++
      if (count % 100 === 0) {
        console.log(`Fetched ${count} files...`)
      }
    })
    files.on('end', () => {
      resolve(null)
    })
  })
  const fileCount = fileList.length
  console.log(`Total files: ${fileCount}`)
  let filesDownloaded = 0
  const promises = fileList.map(async objInfo => {
    if (objInfo.name == null) return
    const fullPath = `${savePath}${objInfo.name.replace(folderName, '')}`
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
    console.log(`Downloading ${objInfo.name} to ${fullPath}`)
    const fileStream = await client.getObject(bucketName, objInfo.name)
    const write = createWriteStream(fullPath, {
      flags: 'w',
      encoding: 'binary'
    })
    fileStream.pipe(write)
    await new Promise<void>((resolve, reject) => {
      write.on('finish', resolve)
      write.on('error', reject)
    })
    filesDownloaded++
    console.log(`Downloaded ${filesDownloaded} of ${fileCount}: ${objInfo.name}`)
    await client.removeObject(bucketName, objInfo.name)
    console.log(`Deleted ${objInfo.name}`)
  })
  await Promise.all(promises)
  console.log(`Downloaded ${filesDownloaded} of ${fileCount} files`)
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
