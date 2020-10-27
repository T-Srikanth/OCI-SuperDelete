import oci

def DeleteBuckets(config, Compartments):
    AllBuckets = []
    object =oci.object_storage.ObjectStorageClient(config)

    ns = object.get_namespace().data

    print ("Getting all buckets for: {}".format(ns))

    for Compartment in Compartments:
        items = object.list_buckets(namespace_name=ns, compartment_id=Compartment.id).data
        if len(items) > 0:
            AllBuckets.append(items)

    for buckets in AllBuckets:
        for bucket in buckets:
            versioning=object.get_bucket(namespace_name=ns, bucket_name=bucket.name).data.versioning
            if versioning != "Disabled":
                DeleteVersionedObjects(config,bucket)
            else:
                DeleteObjects(config,bucket)

    for buckets in AllBuckets:
        for bucket in buckets:
            print ("Delete bucket: {}".format(bucket.name))
            object.delete_bucket(namespace_name=bucket.namespace,bucket_name=bucket.name)


def DeleteObjects(config, bucket):
    objectlimit = 20
    object = oci.object_storage.ObjectStorageClient(config)
    print ("Deleting objects in bucket: {}".format(bucket.name))
    more = True
    np = ""

    while more:
        result = object.list_preauthenticated_requests(namespace_name=bucket.namespace, bucket_name=bucket.name, limit=objectlimit)
        items = result.data
        for item in items:
            print ("Deleting {}:{}".format(bucket.name, item.name))
            object.delete_preauthenticated_request(namespace_name=bucket.namespace, bucket_name=bucket.name, par_id=item.id)

        if len(items) == objectlimit:
            more = True
        else:
            more = False

    more=True

    while more:
        items=object.list_multipart_uploads(namespace_name=bucket.namespace, bucket_name=bucket.name).data
        for item in items:
            object.abort_multipart_upload(namespace_name=bucket.namespace, bucket_name=item.bucket, object_name=item.object, upload_id=item.upload_id)

        if len(items) == objectlimit:
            more = True
        else:
            more = False

    more = True

    while more:
        result = object.list_objects(namespace_name=bucket.namespace, bucket_name=bucket.name, limit=objectlimit)
        items = result.data.objects

        for item in items:
            print ("Deleting {}:{}".format(bucket.name, item.name))
            object.delete_object(namespace_name=bucket.namespace, bucket_name=bucket.name, object_name=item.name)

        if len(items) == objectlimit:
            more = True
        else:
            more = False

def DeleteVersionedObjects(config, bucket):
    objectlimit = 20
    object = oci.object_storage.ObjectStorageClient(config)
    print ("Deleting objects in bucket: {}".format(bucket.name))
    more = True

    while more:
        result = object.list_preauthenticated_requests(namespace_name=bucket.namespace, bucket_name=bucket.name, limit=objectlimit)
        items = result.data
        for item in items:
            print ("Deleting {}:{}".format(bucket.name, item.name))
            object.delete_preauthenticated_request(namespace_name=bucket.namespace, bucket_name=bucket.name, par_id=item.id)

        if len(items) == objectlimit:
            more = True
        else:
            more = False

    more=True

    while more:
        items=object.list_multipart_uploads(namespace_name=bucket.namespace, bucket_name=bucket.name).data
        for item in items:
            object.abort_multipart_upload(namespace_name=bucket.namespace, bucket_name=item.bucket, object_name=item.object, upload_id=item.upload_id)

        if len(items) == objectlimit:
            more = True
        else:
            more = False

    more = True

    while more:
        result = object.list_object_versions(namespace_name=bucket.namespace, bucket_name=bucket.name, limit=objectlimit)
        items = result.data.items

        for item in items:
            print ("Deleting {}:{}".format(bucket.name, item.name))
            print(item.version_id)
            object.delete_object(namespace_name=bucket.namespace, bucket_name=bucket.name, object_name=item.name, version_id=item.version_id)

        if len(items) == objectlimit:
            more = True
        else:
            more = False                  










