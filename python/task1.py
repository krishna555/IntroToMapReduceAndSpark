import json
from pyspark import SparkContext, SparkConf
import sys

class SparkHomework1:
    def run_task_A(self, review_data_rdd):
        return review_data_rdd.count()

    def run_task_B(self, review_data_rdd, year):
        review_cnt_for_year = review_data_rdd.map(lambda json_record: json_record['date'][:4])\
            .filter(lambda data_year: int(data_year) == year)\
            .count()
        return review_cnt_for_year

    def run_task_C(self, review_data_rdd):
        extracted_user_ids = review_data_rdd.map(lambda json_record: json_record['user_id'])
        return extracted_user_ids.distinct().count()

    def run_task_D(self, review_data_rdd, m):
        m_users_with_most_reviews = review_data_rdd.map(lambda json_record: (json_record['user_id'], 1))\
            .reduceByKey(lambda val1, val2: val1 + val2)\
            .takeOrdered(m, key=lambda record_tup: [-record_tup[1], record_tup[0]])
        return m_users_with_most_reviews

    def run_task_E(self, review_data_rdd, n, stop_words_set):

        punctuations = set(["(", "[", ",", ".", "!", "?", ":", ";", "]", ")"])
        def remove_punctuations(text):
            res = []
            for ch in text:
                if ch in punctuations:
                    res.append(' ')
                elif ch.isalpha() or ch == ' ':
                    res.append(ch)
                else:
                    res.append(' ')
            return "".join(res)

        top_n_frequent_words = review_data_rdd.map(lambda json_record: remove_punctuations(json_record["text"]))\
            .flatMap(lambda text: text.split())\
            .map(lambda word: (word.lower(), 1))\
            .reduceByKey(lambda val1, val2: val1 + val2)\
            .filter(lambda keyVal: keyVal[0] != None and keyVal[0] != '' and keyVal[0] not in stop_words_set)\
            .takeOrdered(n, key=lambda record_tup: -record_tup[1])
        return [word_and_count[0] for word_and_count in top_n_frequent_words]

    def main(self):
        conf = SparkConf().setAppName("Task1").setMaster("local[*]")
        sc = SparkContext(conf = conf).getOrCreate()
        sc.setLogLevel("OFF")
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        stopwords_path = sys.argv[3]
        year = int(sys.argv[4])
        m = int(sys.argv[5])
        n = int(sys.argv[6])
        
        with open(stopwords_path, "r") as f:
            stop_words = f.readlines()
            stop_words_set = set([word.rstrip() for word in stop_words])
        output = {}
        review_data_rdd = sc.textFile(input_path).map(lambda row: json.loads(row)).cache()

        output["A"] = self.run_task_A(review_data_rdd)
        output["B"] = self.run_task_B(review_data_rdd, year)
        output["C"] = self.run_task_C(review_data_rdd)
        output["D"] = self.run_task_D(review_data_rdd, m)
        output["E"] = self.run_task_E(review_data_rdd, n, stop_words_set)
        
        with open(output_path, "w+") as of:
            json.dump(output, of)
        of.close()

if __name__ == "__main__":
    hw1 = SparkHomework1()
    hw1.main()