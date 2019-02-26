from pyspark import SparkContext,SparkConf
from math import sqrt
scoreThreshold = 0.97
coOccurenceThreshold = 50
movieId=61
conf=SparkConf().setMaster("local[*]").setAppName("Movie Recomendation")
sc=SparkContext().getOrCreate(conf=conf)

def user_movie_rating(line):
    fields=line.split("\t")

    return (int(fields[0]),(int(fields[1]),int(fields[2])))

def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)
def filter_movie_based_on_name_and_threshold(data):
    if(data[0][0]==movieId or data[0][1] == movieId ) and (data[1][0]>=scoreThreshold) and data[0][1]>coOccurenceThreshold:
        return True
    else:
        return False

def filter_duplicate(data):
    movieOneId=data[1][0][0]
    movieTwoId=data[1][1][0]
    return movieOneId<movieTwoId
def movie_rating_pair_mapper(data):
    data=data[1]
    movies=(data[0][0],data[1][0])
    ratings=(data[0][1],data[1][1])
    return movies,ratings
data=sc.textFile("ml-100k/u.data")
user_movie_rating_rdd=data.map(user_movie_rating)
#user_movie_rating_rdd.foreach(lambda x:print(x))
joinedRatings=user_movie_rating_rdd.join(user_movie_rating_rdd)
joinedRatings_unique=joinedRatings.filter(filter_duplicate)
movie_rating_pair=joinedRatings_unique.map(movie_rating_pair_mapper)
movie_rating_pair_all_by_movie_id=movie_rating_pair.groupByKey()
movie_pair_similarities=movie_rating_pair_all_by_movie_id.mapValues(computeCosineSimilarity).cache()

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item", encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames
nameDict=loadMovieNames()
results=movie_pair_similarities.filter(filter_movie_based_on_name_and_threshold).map(lambda x:(x[1],x[0])).sortByKey(ascending=False).top(10)
for result in results:
    (sim, pair) = result
    similarMovieID = pair[0]
    if (similarMovieID == movieId):
        similarMovieID = pair[1]
    print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))



