pool = redis.ConnectionPool(host='smzdm_bi_rec_cache_redis_m01', port=6379)

r = redis.Redis(connection_pool=pool)
pipe = r.pipeline()
file_size = os.path.getsize(sys.argv[1])
if file_size != 0:
    file = open(sys.argv[1], 'r')
    batch_size = 0
    for line in file.readlines():
        batch_size += 1
        timeout = 4 * 86400
        lines = line.strip("\n")
        keys, cate, brand, tag = lines.split("\001")
        brand
        key = "user_proxy_key:" + keys
        all_data = {
            "cate": {
                set(cate.split(","))
            },
            "brand": {
                set(brand.split(","))
            },
            "tag": {
                set(tag.split(","))
            }
        }
        r.set(key, json.dumps(all_data, cls=SetEncoder), timeout)

        if batch_size == 5000:
            print datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            pipe.execute()
            batch_size = 0
    pipe.execute()
else:
    print sys.argv[1] + "is null"
