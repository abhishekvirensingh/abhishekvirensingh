from pyspark import SparkContext

sc = SparkContext("local[*]", "get_top_course")
sc.setLogLevel("ERROR")

chapters = sc.textFile("C:/Users/abhsingh/Downloads/chapters-201108-004545.csv")
views_1 = sc.textFile("C:/Users/abhsingh/Downloads/views1-201108-004545.csv")

# to find out how many Chapters are there per course :

# column names in chapters : chapter_id,course_id

map_chapters_data = chapters.map(
    lambda x: (str(x.split(",")[1]), 1))  # Build an RDD containing a key of course Id together with the number of
# chapters on that course
count_of_chapters_rdd = map_chapters_data.reduceByKey(lambda x, y: x + y)
count_of_chapters_sorted = count_of_chapters_rdd.map(lambda x: (x[0], x[1])).sortByKey()
count_of_chapters = count_of_chapters_sorted.collect()
for aline in count_of_chapters[0:5]:  # Check the sample of the final result
    print(aline)


# produce a ranking chart detailing based on which are the most popular courses by score.

# get score based on percentage :

def get_score_data(course_id_percentage):
    if course_id_percentage[1] > 0.90:
        return str(course_id_percentage[0]), 10.0
    elif 0.50 < course_id_percentage[1] < 0.90:
        return str(course_id_percentage[0]), 4.0
    elif 0.25 < course_id_percentage[1] < 0.50:
        return str(course_id_percentage[0]), 2.0
    elif course_id_percentage[1] < 0.25:
        return str(course_id_percentage[0]), 0.0
    else:
        return str(course_id_percentage[0]), 0.0


# userId,chapterId,dateAndTime are the columns present in Views.csv file

chapter_data = chapters.map(lambda x: x.split(","))
# join views and chapter RDD on chapter_id
# the same user watching the same chapter doesn't count,so we can call distinct to remove duplication
user_id_chap_id = views_1.map(lambda x: (x.split(",")[1],
                                         x.split(",")[0]
                                         )).distinct().join(chapter_data)

chapter_id_user_id_course_id = user_id_chap_id.map(lambda x: (x[0], x[1][0], x[1][1]))  # split output from join
user_id_course_id = chapter_id_user_id_course_id.map(lambda x: (x[1], x[2]))
user_id_course_id_count = user_id_course_id.map(lambda x: (x, 1)).sortBy(lambda x: x[0][0])
user_id_course_id_view_count = user_id_course_id_count.reduceByKey(lambda x, y: x + y)
course_id_view_count = user_id_course_id_view_count.map(lambda x: (x[0][1], x[1]))
course_id_view_count_chapters = course_id_view_count.join(count_of_chapters_sorted)
percentage_watched = course_id_view_count_chapters.map(lambda x: (x[0], (x[1][0] / x[1][1])))
score_data = percentage_watched.map(get_score_data)
final_score = score_data.reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1])
for aline in final_score.collect():
    print(aline)
