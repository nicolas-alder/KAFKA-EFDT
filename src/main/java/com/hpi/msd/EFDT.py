import pandas as pd
import numpy as np
import time
import string
import requests
import json
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
from tqdm import tqdm
from sklearn import preprocessing

#required functions for the EFDT class

pd.set_option('mode.chained_assignment', None)
lb = preprocessing.LabelBinarizer()

def check_training_status():
    url = 'http://localhost:7070/messages/status'
    response = requests.get(url)
    
    return json.loads(response.content)


def insert_record_rest(insert_record):
    url = 'http://localhost:7070/messages/insert/' + insert_record
    response = requests.get(url)

    return json.loads(response.content)


def send_record_rest(test_record):
    url = 'http://localhost:7070/messages/query/' + test_record
    response = requests.get(url)

    return json.loads(response.content)


def calculate_accuracy(ground_truth, result):
    a = np.array(ground_truth)
    b = np.array(result)

    return np.sum(a == b) / len(a)


def transform_train_data(data):
    Cols = np.array(data.columns.difference(['label'], sort=False))
    Nums = np.array(data.loc[:, data.columns != 'label'])
    Labs = np.array(data.label)
    df = pd.DataFrame((Cols + "_" + Nums).T + "_" + Labs).T
    df_final = transform_to_records(df)

    return df_final


def transform_test_data(data):
    result = []
    Cols = np.array(data.columns)
    Nums = np.array(data)
    df = pd.DataFrame(Cols + "_" + Nums)
    for index, row in df.iterrows():
        result.append("{" + str(list(row)).replace("'", "")[1:-1] + "}")

    return result


def transform_to_records(df):
    record_list = []
    for index, row in df.iterrows():
        record = {}
        for i in row:
            if i[-1] == "0":
                record["label_0_0"] = 1.0
                break
            elif i[-1] == "1":
                record["label_1_1"] = 1.0
                break
        for i in row:
            record[i] = 1.0
        record_list.append(record)

    return record_list


def col_names(data):
    column_names = []
    for i in range(data.shape[1] - 1):
        column_names.append("Attribute{}".format(i))
    column_names.append("label")

    return column_names


def categorize(data,bins):
    names=list(string.ascii_lowercase)
    data_disc=data.loc[:, data.columns != 'label'].apply(lambda x: pd.cut(x,bins,labels=names[:bins]))
    data_disc["label"]=data["label"]

    return data_disc



class EFDT:
    ''' Class containing the most important functionalities to execute the algorithm

        Functions:
            preprocess - transforms the data set into the required form for the algorithm
            train_model - uses training data to build the tree structure
            predict - classifies the observation based on the tree structure
            evaluate_model - evaluates the model using test data
            calc_error_curve - calculates the accuracy of the algorithm as a function of the percentage of data quantity
            plot_error_curves - visualize the results of one or two calc_error_curve lists
        '''


    def preprocess(self,data,name,categorized=False,bins=0,col_names_exist=True, shuffle=True, save_categorized_data=False):
        '''
        parameter:
        data - data that needs to be preprocessed
        categorized - "True" if data is already categorized, "False" if not
        bins - number of equal-width bins
        col_names_exist - "True" if data has column names, "False" if not
        shuffle - "True" if data should be shuffled, "False" if not
        save_categorized_data - "True" if categorized data set should be saved into the format name_categorized to pass it in java jar , "False" if not

        returns:
            train_records - records to train the algorithm
            test_records - records to evaluate the algorithm
            ground_truth - reference labels for evaluation
        '''

        data.dropna()
        if col_names_exist==False:
            data.columns = col_names(data)
        if categorized==False:
            if bins==0:
                print("Please specifiy the number of bins first")
                return
            data = categorize(data, bins)
        data.label = lb.fit_transform(np.array(data.label))

        if save_categorized_data==True:
            data.to_csv("{}_categorized.csv".format(name.split("/")[-1].split(".")[0]),header=True,index=False)

        data.label = data.label.apply(str)
        data_train, data_test = train_test_split(data, test_size=0.1, shuffle=shuffle, stratify=None)
        train_record_list = transform_train_data(data_train)

        ground_truth = data_test.label.apply(lambda x: int(float(x)))
        data_test.drop("label", axis=1, inplace=True)
        test_record_list = transform_test_data(data_test)

        return train_record_list, test_record_list, ground_truth


    def train_model(self,train_records):
        '''
        parameter:
            train_records - records to train the algorithm

        returns:
            String "done" when completed
        '''

        for record in train_records:
            input = json.dumps(record).replace(":", "=").replace("\"", "").replace("= 1.0", "")
            insert_record_rest(input)

        while check_training_status()==1:
            time.sleep(3)

        return "done"


    def predict(self,test_records):
        '''
        parameter:
            test_records - records which should predicted

        returns:
            list of predicted labels
        '''

        results=[]
        for record in test_records:
            results.append(send_record_rest(record))

        return results


    def evaluate_model(self,test_records, ground_truth):
        '''
        parameter:
            test_records - records to evaluate the algorithm
            ground_truth - reference labels for evaluation

        returns:
            value of accuracy
        '''

        result = []
        for record in test_records:
            result.append(send_record_rest(record))
        result_final = pd.DataFrame(ground_truth)
        result_final["predicted"] = result
        accuracy = calculate_accuracy(result_final.label, result_final.predicted)

        return accuracy


    def calc_error_curve(self,train_records, test_records,ground_truth, percentage_split=0.01):
        '''
        parameter:
            train_records - records to train the algorithm
            test_records - records to evaluate the algorithm
            ground_truth - reference labels for evaluation
            percentage_split - percentage into which the data is to be divided

        returns:
            list of accuracy values
        '''

        accuracies = []
        instances_perc = [int(x) for x in np.linspace(0, 1, int(1 / percentage_split) + 1) * len(train_records)]
        step_width = instances_perc[1]
        for i in tqdm(instances_perc):
            if i != instances_perc[-1]:
                chunk = train_records[i:i + step_width]
                self.train_model(chunk)
                while check_training_status() == 1:
                    time.sleep(3)
                accuracy = self.evaluate_model(test_records, ground_truth=ground_truth)
                print(accuracy)
                accuracies.append(accuracy)

        return accuracies

    def plot_error_curves(self,len_train_records,name, accuracies_shuffled=None, accuracies_unshuffled=None, save=True):
        '''
        parameter:
            name - unique name to save the result and to insert into the title
            len_train_records - number of train records
            accuracies_shuffled - result of calc_error_curve if data is shuffled
            accuracies_unshuffled - result of calc_error_curve if data is unshuffled
            save - "True" if plot should be saved, "False" if not

        returns:
            visualization of the error curves
        '''

        plt.figure(figsize=(20, 10))
        plt.title("{} Data Error Curve".format(name.split("/")[-1].split(".")[0]), fontsize=20)
        plt.xlabel("Instances", fontsize=15)
        plt.ylabel("Error rates", fontsize=15)

        if accuracies_unshuffled==None:
            z = np.linspace(0, len_train_records, len(accuracies_shuffled))
            errors = [1 - x for x in accuracies_shuffled]
            plt.plot(z, errors, ":", label="shuffled", linewidth=3)

        elif accuracies_shuffled==None:
            z = np.linspace(0, len_train_records, len(accuracies_unshuffled))
            errors = [1 - x for x in accuracies_unshuffled]
            plt.plot(z, errors, ":", label="shuffled", linewidth=3)

        elif accuracies_unshuffled==None and accuracies_shuffled==None:
            print("Please use the function calc_error_curves first")

        else:
            z = np.linspace(0, len_train_records, len(accuracies_shuffled))
            errors_shuffled = [1 - x for x in accuracies_shuffled]
            errors_unshuffled = [1 - x for x in accuracies_unshuffled]
            plt.plot(z, errors_shuffled, ":", label="shuffled", linewidth=3)
            plt.plot(z, errors_unshuffled, ":", label="unshuffled", linewidth=3)
            plt.legend(fontsize=15)

        plt.ylim(0, 1)
        plt.xlim(0, z[-1])

        if save==True:
            plt.savefig("Error_Curve_{}.png".format(name.split("/")[-1].split(".")[0]))
            plt.savefig("Error_Curve_{}.pdf".format(name.split("/")[-1].split(".")[0]))


        plt.show()


