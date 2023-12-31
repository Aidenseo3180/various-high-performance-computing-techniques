#include <iostream>
#include <pthread.h>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <cmath>

//declare the number threads that we're going to use from here
//used define so that whenever we make changes to it, simply change this number
#define THREAD_NUM 5

using namespace std;

/*
 * ****************************************************** 
 * 
 * Data Parallelism
 * 
 * How I approached segmenting data into pieces:
 * I created a struct called task that can have indices of when each month starts & ends.  These indices will be used on "text_input", a vector of string
 * that saves all the string lines of the input file except for anomalies & new line
 * Each thread will use this task to access certain index of this vector of string to retrieve all the temperatures (for each sec) for each month
 * Each thread will be given task that has information of each month --> so none of the threads will access the same month or same data chunks
 * This ensures that all the threads will be working on their own data segments independently
 * 
 * [Below info are already mentioned in serial]
 * Note before getting started:
 * I have decided cooling months as: May, June, July, August
 * I have decided heating months as: October, November, December, January, February
 * March, April, and September are not considered and therefore, ignored
 * 
 * Note on how I decided to find over-heating and over-cooling:
 * I first read the whole month, find typical temperature (average) of that month and its standard deviation, and save them in map.
 * Also, save all the valid lines (ignore newlines and anomalies) of input file in a vector of string.
 * Then in the 2nd run with vector of string, I use this average with stdev to determine over-cooling & over-heating status of each hour by comparing each temperature
 * with what's in the map depending on what month we're currently in (only for either cooling or heating month)
 * 
 * *******************************************************
*/

/*
 **********************************************************
 * 
 * This is the task that each thread gets
 * It has start index and end index of when each month ends in "text_input" vector of string that saves ALL the valid temperatures from input file
 * Each thread uses this to access certain data points saved in the vector, similar to how the data is chunked and is read by threads
 * These indices will never overlap with other threads, so each chunk of data is read independently by each thread
 * 
 * 
 * I declared them as unsigned long type because the number of lines goes up to 59 million lines for the smallest A version of input file 
 * and the unsigned long type ranges from 0 to 4.3 billion, which is more than enough. I have also tested and confirmed that it can handle the 
 * biggest 2.6GB file given from the class. This struct was used as a type of task that the threads will be assigned to so that each thread 
 * can use these indices to know which part of the vector that stores the input file by simply using random access. 
 * 
 ***********************************************************
*/
typedef struct Task{
    //start_idx for start date, end_idx for end date of specific month
    unsigned long start_idx, end_idx;
    //struct constructor
    Task(){};
    Task(unsigned long start, unsigned long end){
        start_idx = start;
        end_idx = end;
    };
}Task;

//IMPORTANT: I've set all of these variables as global because the threads need to use them as well from a separate method call

//task queue is the queue that has all the tasks. Each task = start and end index of each month for text_input vector of string
//So 256 can cover a little more than all months of 21 years, which is more than enough (21 x 12 = 252)
Task task_queue[256];
//task_count to keep the total number of tasks inside the queue
int task_count = 0;

//Since anything in the task queue and output file are a critical section (bc overwriting or loss of data can happen), need mutex lock
pthread_mutex_t mutex_queue;    //mutex lock for task queue bc we have to allow only 1 thread to rearrange queue
pthread_mutex_t mutex_file;     //mutex lock for output file bc we have to allow only 1 thread to write to output file

//store each lines of the text
/*
 * since the maximum size of the vector of string is at least 2^42 – 1 (2^(64 – size(string))-1 where size of string = 22 per line in input file) 
 * in 64bit OS, I knew for sure that it wouldn’t throw the overflow during the operation. I have already confirmed that storing input text file as 
 * vector of string works even for the biggest 2.6GB file given to the class.
*/
vector<string> text_input;

//save stdev high and low for all months of all years
unordered_map<string, unordered_map<string, float> > stdev_high_per_year;   //{year, {month, mean + stdev}}
unordered_map<string, unordered_map<string, float> > stdev_low_per_year;    //{year, {month, mean - stdev}}

//use pointer(*task) bc we dont want to create a copy of it
//each thread will perform this method to work on task
void* execute_task(Task* task){

    //save previous hour to keep a track of when the hour changes from one to another
    string prev_hour = "";
    vector<string> res;

    //indiciate when to skip. Will use to skip and read the next hour instead of reading the next second if heating or cooling has been found within that hour
    //since we want to go to next hour once we find out that current hour is over-heating or over-cooling, set a flag and skip until next hour is found
    bool skip_flag = false;
    for (int i = task->start_idx; i <= task->end_idx; i++){
        //using random access, retrieve each line more quickly
        string line = text_input[i];

        //using stringstream, I separate the line of string into 3 sections (in array) based on the whitespace
        //so the line gets separated by 3 sections: date, time, and temperature
        //Ex. "06/05/04 01:59:38 67.8" -> date, time, temperature
        stringstream ss(line);
        string word;
        //index to indicate where each section of the line should be stored to an array of string named each_line
        int idx = 0;
        string each_line[3];
        while(getline(ss, word, ' ')){  //separate the line into 3 segments using spaces
            each_line[idx] = word;
            idx++;
        }

        //find the year
        string curr_year = each_line[0].substr(6, 2);
        //find the month
        string curr_month = each_line[0].substr(0, 2);
        //find the hour
        string curr_hour = each_line[1].substr(0, 2);
        //current temperature
        float curr_temp = stod(each_line[2]);

        //if we're in different hours, then turn off skip flag and let the program run through each seconds of the hour
        if (prev_hour != curr_hour){
            skip_flag = false;
            prev_hour = curr_hour;
        }

        //skip if heating or cooling time already found within the same hour time interval
        if (skip_flag == true){
            continue;
        }

        //if current month is May to August (cooling months)
        if (curr_month == "05" || curr_month == "06" || curr_month == "07" || curr_month == "08"){
            //check for hours when too much cooling going on by reading stdev_low_per_year using current year and current month as indices
            if (curr_temp < stdev_low_per_year[curr_year][curr_month]){
                res.push_back(line + " - temp too cold, one stdev lower: " + to_string(stdev_low_per_year[curr_year][curr_month]));
                //since over-cooling hour is found, set skip flag to true
                skip_flag = true;
            }
        }
        //if current month is October to February (heating months)
        else if (curr_month == "10" || curr_month == "11" || curr_month == "12" || curr_month == "01" || curr_month == "02"){
            //check for hours when too much heating going on by reading stdev_high_per_year using current year and current month as indices
            if (curr_temp > stdev_high_per_year[curr_year][curr_month]){
                res.push_back(line + " - temp too warm, one stdev higher: " + to_string(stdev_high_per_year[curr_year][curr_month]));
                //since over-heating hour is found, set skip flag to true
                skip_flag = true;
            }
        }
    }

    //set mutex lock to prevent multiple threads from writing to the same file at the same time, preventing sync issues
    pthread_mutex_lock(&mutex_file);

    //append to an existing file using ios_base::app
    ofstream output_file("output_data_parallel.txt", ios_base::app);
    if (output_file.is_open()){
        //read the res vector where we saved all the over-cooling & over-heating temperatures
        for (int i = 0; i < res.size(); i++){
            output_file << res[i] << "\n";
        }
        //close the output file after done writing
        output_file.close();
    }
    //release the lock so that other threads can write to the output file now
    pthread_mutex_unlock(&mutex_file);
}

//function that starts the threads and call to pick up the task from the task queue
void *start_thread(void* args){

    //thread either waits or execute the task, so it's not gonna terminate
    while(1){
        //since anything inside task queue = critical section, lock mutex when task queue is in use
        pthread_mutex_lock(&mutex_queue);

        Task task;
        int found = 0;          //flag that checks the existence of task
        //if task_count is > 0, then there's a task
        if (task_count > 0){
            found = 1;
            //read the first task inside the queue
            task = task_queue[0];
            //since using array is faster than queue when it comes to small size, use array like queue
            //after reading the first task of queue, shift all the elements by 1 to the left
            for (int i = 0; i < task_count - 1; i++){
                task_queue[i] = task_queue[i + 1];
            }
            //decrement the number of tasks inside the queue
            task_count--;
        }
        //if reaches the end of queue (if queue is empty and there's no task_count), unlock mutex and break out -> let thread terminate
        else{
            pthread_mutex_unlock(&mutex_queue);
            break;
        }

        //when queue is not in use, free mutex
        pthread_mutex_unlock(&mutex_queue);

        //if we know that there's a task, then execute it
        if (found == 1){
            //execute the task here. Comes after mutex because we free mutex and then execute (execution is not a part of critical section)
            execute_task(&task);
        }
    }
}

int main(){
    
    //read input stream
    //IMPORTANT! I have changed the input text file name as "bigw12a_log.txt" from "bigw12a.log.txt" to make sure that I am giving the file as text file to the program
    ifstream file("bigw12a_log.txt");

    //create output file that I'll be writing all the over-heating and over-cooling time
    ofstream output_file("output_data_parallel.txt");
    //close it to save resource. Only open the output file when writing to it
    output_file.close();

    /*
     * **********************************************************
     * 
     * First, read the file from the beginning until the end and find average & stdev & task
     * Process is the same as the one in serial version except that now we save tasks at the END of each month
     * 
     * ***********************************************************
    */
   
    auto beg = std::chrono::high_resolution_clock::now();
    if (file.is_open()){
        string line; 
        
        //variables to store typical temperature of month per year
        unordered_map<string, float> typical_temp_per_month;   //takes {month, total typical temp}

        //read all the lines, find typical_temp
        float prev_temp = 0;
        string prev_month = "";
        string prev_year = "";
        //temporarily store temperatures of all the days within a specific month
        vector<float> temp_list;
        unsigned long month_start_idx = 0, month_end_idx = 0;
        while(getline(file, line)){
            //skip blank line
            if (line == "")
                continue;

            //using stringstream, I separate the line of string into 3 sections (in array) based on the whitespace
            //so the line gets separated by 3 sections: date, time, and temperature
            //Ex. "06/05/04 01:59:38 67.8" -> date, time, temperature
            stringstream ss(line);
            string word;
            int idx = 0;
            //stores date, time, and temperature
            string each_line[3];
            while(getline(ss, word, ' ')){
                each_line[idx] = word;
                idx++;
            }

            //find the year
            string curr_year = each_line[0].substr(6, 2);
            //find month, use as index
            string curr_month = each_line[0].substr(0, 2);
            //current time temperature
            float curr_temp = stod(each_line[2]);

            //NOTE assume these months are the months that really don't need any heating and cooling (to save time)
            if (curr_month == "03" || curr_month == "04" || curr_month == "09"){
                prev_temp = 0;  //set prev temp to 0 to prevent 
                continue;
            }

            //check for any anomalies. If exist, skip
            //I placed prev_temp = curr_temp after this if statement so that prev_temp stays the same when anomaly happens
            if (prev_temp + 2 < curr_temp || prev_temp - 2 > curr_temp){
                if (prev_temp != 0)
                    continue;
            }

            //save curr_temp as prev_temp for the future loop
            prev_temp = curr_temp;

            //after skipping anomalies & blank line, save the text input for later use
            text_input.push_back(line);

            //if saved date is different, then date has been changed. Therefore, find average (typical temp) of that month
            if (prev_month != curr_month){
                if (prev_month != ""){
                    //when you find out that you're in different month, calculate standard deviation of prev month
                    
                    //---------find standard deviation-----------

                    //average
                    typical_temp_per_month[prev_month] = typical_temp_per_month[prev_month] / temp_list.size();

                    //stdev
                    float result_val = 0;
                    int n = temp_list.size();
                    for (int i = 0; i < n; i++){
                        result_val += pow(temp_list[i] - typical_temp_per_month[prev_month], 2);
                    }
                    float stdev = sqrt(result_val / n);

                    //-------------------------------------------

                    //save one stdev higher & one stdev lower for each year, each month
                    //so that whenever I need it, I can go to either of these 2 maps & retrieve the data that's appropriate for either heating or cooling month
                    stdev_high_per_year[prev_year][prev_month] = typical_temp_per_month[prev_month] + stdev;
                    stdev_low_per_year[prev_year][prev_month] = typical_temp_per_month[prev_month] - stdev;

                    temp_list.clear();
                    typical_temp_per_month.clear();

                    //if the prev month and curr month are different, save indices of when prev month starts and ends
                    month_end_idx = text_input.size() - 2;
                    task_queue[task_count] = Task(month_start_idx, month_end_idx);
                    //count up the task because new task is going into the task queue. Task_count also used as an index to save the task
                    task_count++;
                    //and then set the new month starting idx as size - 1 (because size starts from 1 and we want to use it as index of vector)
                    month_start_idx = text_input.size() - 1;
                }
                //since month has changed, save curr_month to prev_month
                prev_month = curr_month;

                //if in different month, then we might be in different year as well
                //if we're in different year or prev_year is not yet initialized, save prev_year as curr_year
                if (prev_year != curr_year){
                    prev_year = curr_year;
                }
            }

            //just keep adding the temperature within the same month to find the mean when the month changes
            typical_temp_per_month[curr_month] += curr_temp;
            //save temp_list because we want to know the number of elements within that month using this
            temp_list.push_back(curr_temp);
        }

        //------if reached the end of file, save the progress of the current month-------
        //average
        typical_temp_per_month[prev_month] = typical_temp_per_month[prev_month] / temp_list.size();

        //stdev
        float result_val = 0;
        int n = temp_list.size();
        for (int i = 0; i < n; i++){
            result_val += pow(temp_list[i] - typical_temp_per_month[prev_month], 2);
        }
        float stdev = sqrt(result_val / n);

        //save average + stdev & average - stdev to map
        stdev_high_per_year[prev_year][prev_month] = typical_temp_per_month[prev_month] + stdev;
        stdev_low_per_year[prev_year][prev_month] = typical_temp_per_month[prev_month] - stdev;

        //also take account of the last month into task queue, saving start and end date of the last month in output file
        month_end_idx = text_input.size() - 1;
        task_queue[task_count] = Task(month_start_idx, month_end_idx);
        task_count++;

        file.close();
    }

    /*
    ************************************************************
    *   
    * THREADS 
    * 
    * Create 5 threads, give task (indices of when each month starts & ends to each threads) to deal with (chunk of data)
    * 
    * Before the creation of threads, mutex locks that will be used to ensure that only one thread is working on the critical section must be initialized. 
    * If the locks are not set up properly, it leads to threads accessing and modifying the same memory that is being shared at the same time, 
    * causing the data to be lost or overwritten. During the thread creation, the thread gets created with the function that they’re assigned to work on. 
    * In this case, it is the start_thread method that they’re calling. Then, during the thread termination, after these threads finish executing 
    * all the tasks inside the task queue, they will break out of their wait state and call pthread_join() method to terminate. 
    * After all the threads have been terminated, the mutex locks also has to be destroyed. 
    * 
    ************************************************************
    */
    //threads
    //THREAD_NUM is a defined variable
    pthread_t ids[THREAD_NUM];
    //initialize all the mutex locks because we're gonna use them now
    //Mutex_queue to lock the task queue, mutex_file to lock the output file when making changes to them
    pthread_mutex_init(&mutex_queue, NULL);
    pthread_mutex_init(&mutex_file, NULL);

    for (int i = 0; i < THREAD_NUM; i++){
        //read text input that is saved in vector again and check for heating and cooling using multiple threads
        if (pthread_create(&ids[i], NULL, &start_thread, NULL) != 0 ){
            perror("Failed to create threads");
        }
    }

    for (int i = 0; i < THREAD_NUM; i++){
        //wait for threads to terminate
        if (pthread_join(ids[i], NULL) != 0){
            perror("Failed to join the thread");
        }
    }

    //destroy all the mutex locks at the end because we no longer need them
    pthread_mutex_destroy(&mutex_queue);
    pthread_mutex_destroy(&mutex_file);

    //measure the time
    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - beg).count();

    //append the elapsed_time to an existing file
    ofstream reopen_file("output_data_parallel.txt", ios_base::app);
    if (reopen_file.is_open()){
        reopen_file << "elapsed time for data parallel: " << elapsed_time << " ms\n";
        reopen_file.close();
    }

    return 0;
}