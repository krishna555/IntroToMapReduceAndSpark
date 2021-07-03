import json
from pyspark import SparkContext, SparkConf
import sys
from collections import defaultdict

class Task2:

    def process_no_spark(self, review_file_path, business_file_path, n):
        def readFile(file_path, required_keys):
            with open(file_path) as readF:
                json_records = []
                for line in readF:
                    line = line.strip()
                    json_record = json.loads(line)
                    current_record = {}
                    for key in required_keys:
                        current_record[key] = json_record[key]
                    json_records.append(current_record)
            readF.close()
            return json_records
        review_records = readFile(review_file_path, ['business_id', 'stars'])
        business_records = readFile(business_file_path, ['business_id', 'categories'])
        bid_to_st = defaultdict()

        # Extract Business Id and Stars
        for review_obj in review_records:
            business_id = review_obj['business_id']
            stars = review_obj['stars']
            if business_id not in bid_to_st:
                bid_to_st[business_id] = [stars]
            else:
                bid_to_st[business_id].append(stars)


        # Extract Business Id and Categories
        business_id_to_categories = defaultdict(list)
        for business_obj in business_records:
            categories_string = business_obj['categories']
            business_id = business_obj['business_id']
            if categories_string:
                categories = categories_string.split(',')
                for category in categories:
                    business_id_to_categories[business_id].append(category.strip())

        keys = bid_to_st.keys()
        category_to_stars = defaultdict(list)
        star_sum = defaultdict(int)
        for business_id in bid_to_st.keys():
            stars = bid_to_st[business_id]
            if business_id in business_id_to_categories:
                categories = business_id_to_categories[business_id]
                for category in categories:
                    for star in stars:
                        category_to_stars[category].append(star)

        res = defaultdict()
        ans = []
        for category in category_to_stars.keys():
            res[category] = sum(category_to_stars[category])/len(category_to_stars[category])
            ans.append((category, res[category]))

        ans = sorted(ans, key=lambda x: (-x[1], x[0]))[:n]
        return ans

    def process_with_spark(self, sc, review_file_path, business_file_path, n):

        review_data_rdd = sc.textFile(review_file_path).map(lambda row: json.loads(row))
        business_data_rdd = sc.textFile(business_file_path).map(lambda row: json.loads(row))

        review_bid_stars_rdd = review_data_rdd.map(lambda json_record: (json_record['business_id'], json_record['stars']))
        business_bid_categories_rdd = business_data_rdd.map(lambda json_record: (json_record['business_id'], json_record['categories']))

        stars_and_count_rdd = review_bid_stars_rdd.groupByKey() \
            .map(lambda bid_and_stars: (bid_and_stars[0], (sum(bid_and_stars[1]), len(bid_and_stars[1]))))

        def all_categories_sanitizer(categories):
            res = []
            category_list = categories.split(',')
            for category in category_list:
                res.append(category.strip())
            return res

        categories_rdd = business_bid_categories_rdd \
            .filter(lambda bid_and_categories: bid_and_categories[1] is not None and bid_and_categories[1] != '')

        result = stars_and_count_rdd.join(categories_rdd) \
            .map(lambda tup: (all_categories_sanitizer(tup[1][1]), tup[1][0])) \
            .flatMap(lambda tup: [(category, tup[1]) for category in tup[0]]) \
            .reduceByKey(lambda tup1, tup2: (tup1[0] + tup2[0], tup1[1] + tup2[1])) \
            .mapValues(lambda sum_and_count_tuple: sum_and_count_tuple[0] / sum_and_count_tuple[1]) \
            .takeOrdered(n, key=lambda tup: (-tup[1], tup[0]))

        return result


    def main(self):
        conf = SparkConf().setAppName("Task2").setMaster("local[*]")
        sc = SparkContext(conf = conf).getOrCreate()
        sc.setLogLevel("OFF")
        review_file_path = sys.argv[1]
        business_file_path = sys.argv[2]
        output_file_path = sys.argv[3]
        spark_mode = sys.argv[4]
        n = int(sys.argv[5])

        if spark_mode == "spark":
            output = self.process_with_spark(sc, review_file_path, business_file_path, n)
        else:
            output = self.process_no_spark(review_file_path, business_file_path, n)

        res = []
        for tup in output:
            res.append([tup[0], round(tup[1], 1)    ])

        output_json = {}
        output_json['result'] = res

        with open(output_file_path, "w+") as of:
            json.dump(output_json, of)
        of.close()

if __name__ == "__main__":
    task2 = Task2()
    task2.main()