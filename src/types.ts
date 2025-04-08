export interface MetadataItem {
  Key: string
  Value: string
}
export type Owner = {
  ID: string
  DisplayName: string
}
export type Metadata = {
  Items: MetadataItem[]
}

export type ObjectInfo = {
  key?: string
  name?: string
  lastModified?: Date
  etag?: string
  owner?: Owner
  storageClass?: string
  userMetadata?: Metadata
  userTags?: string
  prefix?: string
  size?: number
}
