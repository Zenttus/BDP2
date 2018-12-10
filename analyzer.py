from pyspark.sql import SparkSession


def get_hashtags(df, time):
    keywordtext = df.select('text')
    words = keywordtext.rdd.flatMap(lambda line: line.split(" "))
    hash_tags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
    total = hash_tags.reduceByKey(lambda x, y: x + y)
    res = ''
    for w in total.top(10, lambda t: t[1]):
        res += w
    res += ', '
    for n in total.top(10, lambda t: t[0]):
        res += str(n)
    res += ', ' + time
    save_results(res, './data/hashtags')


def get_words(df, time):
    keywordtext = df.select('text')
    words = keywordtext.rdd.flatMap(lambda line: line.split(" "))
    w = words.map(lambda x: (x, 1))
    total = w.reduceByKey(lambda x, y: x + y)
    res = ''
    for w in total.top(10, lambda t: t[1]):
        res += w
    res += ', '
    for n in total.top(10, lambda t: t[0]):
        res += str(n)
    res += ', ' + time
    save_results(res, './data/words')


#def get_users(df, time): TODO


#def get_keywords(df, time): TODO



def save_results(res, file, time):
    f = open(file, 'a+')
    f.write(res)
    f.close()


def run():
    f = open('./tweetsList.txt', 'r')
    for line in f.readlines():
        ss = SparkSession.builder.getOrCreate()
        df = ss.read.json(line.split(',')[0])
        time = line.split(',')[1]

        get_hashtags(df, time)
        get_words(df, time)
        #get_users(df, time)
        #get_keywords(df, time)

    f.close()
    # Erase files
    f = open('./tweetsList.txt', 'w')
    f.close()
