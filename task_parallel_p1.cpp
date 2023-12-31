#include <iostream>
#include <pthread.h>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <cmath>
#include <queue>

//declare the number threads that we're going to use from here
//used define so that whenever we make changes to it, simply change this number
#define THREAD_NUM 5

using namespace std;

/*
 * ******************************************************
 * 
 * Task Parallelism
 * 
 * During the input file read, it creates Month Task that lets the thread know when each month starts and ends in a form of a task & put into Month Task queue that will be used for 1st stage!! 
 * (because threads need tasks to begin with)
 * 
 * It is 3 stage pipeline where:
 * 1st stage reads data by each month (Month task created during reading the input text file) -> segments each month by each hour and pass it to 2nd stage as a DateTask
 * 2nd stage reads each hour -> find out whether it's over-heating or over-cooling and pass this information to 3rd stage as another OutputTask
 * 3rd stage reads the OutputTask and Write to output file. Since it's 3 stage pipeline, no task gets created from 3rd stage.
 * 
 * [Below info are already mentioned in serial & data parallel]
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
 * ****************************************************** 
*/

/*
 * Task called "MonthTask" that will be used in 1st stage
 * Saves start index & end index of each month within the vector of string called "text_input"
*/
typedef struct MonthTask{
    unsigned long start_idx, end_idx;
    //struct constructor
    MonthTask(){};
    MonthTask(unsigned long start_idx, unsigned long end_idx){
        this->start_idx = start_idx;
        this->end_idx = end_idx;
    };
}MonthTask;

/*
 * Task called "DateTask" that will be used in 2nd stage
 * Saves the start index and end index of each hour of each month within the vector of string called "text_input"
*/
typedef struct DateTask{
    unsigned long hour_start_idx, hour_end_idx;
    DateTask(){};
    DateTask(unsigned long hour_start_idx, unsigned long hour_end_idx){
        this->hour_start_idx = hour_start_idx;
        this->hour_end_idx = hour_end_idx;
    }
}DateTask;

/*
 * Task called "OutputTask" that will be used in 3rd stage
 * Saves the output string that contains the information of over-cooling & over-heating hour that will be written to the output file
*/
typedef struct OutputTask{
    string output_string;
    OutputTask(){};
    OutputTask(string output_string){
        this->output_string = output_string;
    };
}OutputTask;

//IMPORTANT: I've set all of these variables as global because the threads need to use them as well from a separate method call
//task queue has task where each task = start and end date of each month
//So 256 can cover nearly all months in 21 years, which is more than enough
MonthTask month_task_queue[256];
int month_task_count = 0;     //task_count to move through the task queue above

//since there are 60 mins x 60 sec = 3600 possible input command within 1 hour period, use queue library instead of using array as queue
//since using the array slike a queue is only efficient when it's size is less than 500, I decided to use queue library
queue<DateTask> date_task_queue;

//since there are 24 hours in 1 day, and 30 days in 1 month, it will need at max 24 x 30 = 720 spaces in worst case
//so use queue instead of using array as queue
queue<OutputTask> output_task_queue;

//Since anything that is being shared is considered a critical section, need mutex locks for all of them
//because we do not want to lose data from multiple threads working on the same file or queue at the same time and making changes to them
pthread_mutex_t mutex_month_task_queue;       //lock for rearranging month task queue
pthread_mutex_t mutex_date_queue;           //lock for rearranging date task queue
pthread_mutex_t mutex_output_queue;         //lock for rearranging output task queue
pthread_mutex_t mutex_file;                 //mutex lock for writing to a file

//store each lines of the text
/*
 * since the maximum size of the vector of string is at least 2^42 – 1 (2^(64 – size(string))-1 where size of string = 22 per line in input file) 
 * in 64bit OS, I knew for sure that it wouldn’t throw the overflow during the operation. I have already confirmed that storing input text file as 
 * vector of string works even for the biggest 2.6GB file given to the class.
*/
vector<string> text_input;

//save average + 1 stdev(high) & stdev - 1 stdev(low) for all months of all years
unordered_map<string, unordered_map<string, float> > stdev_high_per_year;   //{year, {month, mean + stdev}}
unordered_map<string, unordered_map<string, float> > stdev_low_per_year;    //{year, {month, mean - stdev}}

//use pointer(*task) bc we dont want to create a copy of it
//this is the function that each thread calls to execute the each of the month tasks
void execute_task(MonthTask* task){

    //save the prev hour to determine when the hour changes to the next one
    //bc i need to know whenever the hour changes so that I can pass it on to the next stage
    string prev_hour = "";

    //you have to set start idx and end idx to task->start_idx because thats where the hour idx is located
    //these variables will be used to determine when each hour starts & end --> and will be passed over to 2nd stage
    unsigned long hour_start_idx = task->start_idx, hour_end_idx = task->start_idx;

    //using each month's start idx & end idx -> we can read data chunks independently that are saved in vector of string called "text_input"
    for (int i = task->start_idx; i <= task->end_idx; i++){
        //using random access, retrieve each line more quickly
        string line = text_input[i];

        //split by space using stringstream
        //using stringstream, I separate the line of string into 3 sections (in array) based on the whitespace
        //so the line gets separated by 3 sections: date, time, and temperature
        //Ex. "06/05/04 01:59:38 67.8" -> date, time, temperature
        stringstream ss(line);
        string word;
        int idx = 0;
        string each_line[3];
        while(getline(ss, word, ' ')){  //separate the line into 3 segments using spaces
            each_line[idx] = word;
            idx++;
        }

        //find the hour
        string curr_hour = each_line[1].substr(0, 2);

        //if the hour has changed
        if (prev_hour != curr_hour){
            //if prev_hour is not empty (since when this for-loop first runs, prev_hour is "" so we have to make sure that prev_hour has a value)
            if (prev_hour != ""){
                //lock it because we're updating this queue
                //Since these hour indices are also getting shared by multiple threads --> it should be treated as critical section (so put in the lock)
                pthread_mutex_lock(&mutex_date_queue);
                hour_end_idx = i - 1;   //update end index
                date_task_queue.push(DateTask(hour_start_idx, hour_end_idx));   //push new date task to a queue
                hour_start_idx = i;     //update start index
                pthread_mutex_unlock(&mutex_date_queue);    //unlock mutex so other threads can update the DateTask queue now
            }
            //set prev hour to current hour and continue
            prev_hour = curr_hour;
        }
    }
}

//For each month, read each day's each time(hour), determine if overheating or overcooling is taking place within that hour
//So, this method reads through all seconds within each hour
//this is the function that each thread calls to work on each date task
void execute_date_task(DateTask* date_task){
    //read all time interval within that specific hour
    for (int i = date_task->hour_start_idx; i <= date_task->hour_end_idx; i++){
        //string that has all the information about that specific time period
        string hour_line = text_input[i];

        //using stringstream, I separate the line of string into 3 sections (in array) based on the whitespace
        //so the line gets separated by 3 sections: date, time, and temperature
        //Ex. "06/05/04 01:59:38 67.8" -> date, time, temperature
        stringstream ss(hour_line);
        string word;
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

        //if current month is May to August (cooling months)
        if (curr_month == "05" || curr_month == "06" || curr_month == "07" || curr_month == "08"){
            //check for hours when too much cooling going on
            if (curr_temp < stdev_low_per_year[curr_year][curr_month]){
                //if over-cooling is happening:
                //assign new task to output task queue. While assigning, need to lock it to prevent other threads to update it at the same time
                pthread_mutex_lock(&mutex_output_queue);
                output_task_queue.push(OutputTask(hour_line + " - temp too cold, one stdev lower: " + to_string(stdev_low_per_year[curr_year][curr_month])));
                pthread_mutex_unlock(&mutex_output_queue);
                //break out and make thread to end checking the rest of the hour because over-cooling is already found
                //we can do this because each DateTask covers each hour --> so if we break, then that stops thread from reading rest of the hour
                break;
            }
        }
        //if current month is October to February (heating months)
        else if (curr_month == "10" || curr_month == "11" || curr_month == "12" || curr_month == "01" || curr_month == "02"){
            //check for hours when too much heating going on
            if (curr_temp > stdev_high_per_year[curr_year][curr_month]){
                //if over-heating is happening:
                //assign new task to output task queue. While assigning, need to lock it to prevent other threads to update it at the same time
                pthread_mutex_lock(&mutex_output_queue);
                output_task_queue.push(OutputTask(hour_line + " - temp too warm, one stdev higher: " + to_string(stdev_high_per_year[curr_year][curr_month])));
                pthread_mutex_unlock(&mutex_output_queue);
                //break out and make thread to end checking the rest of the hour because over-heating is already found
                break;
            }
        }
    }
}

//this is the function that each thread calls to work on output task where output task is to write to the output file
void execute_output_task(OutputTask* output_task){

    //using mutex_file lock, only allow 1 thread to write to the output file at the same time
    pthread_mutex_lock(&mutex_file);
    //append to an existing file by using ios_base::app
    ofstream output_file("output_task_parallel.txt", ios_base::app);
    if (output_file.is_open()){
        output_file << output_task->output_string << "\n";
        output_file.close();
    }
    //unlock so that now other threads can write to it
    pthread_mutex_unlock(&mutex_file);
}

//function that starts the threads and call to pick up the task from the task queue
void* start_thread(void* args){

    //thread either waits or execute the task, so it's not gonna terminate
    //UNLESS all the queues (task queue, date task queue, output task queue are empty)
    while(1){
        /**
         * VERY IMPORTANT!
         * THIS IF STATEMENT IS TO ASSIGN 3RD STAGE TASKS TO THREADS
         * since output_task is the last stage (3rd stage), I gave highest priority to it.
         * therefore, if any works remaining at last stage, it gets handed over to thread first
        */
        if (!output_task_queue.empty()){

            //Use the lock to prevent other threads to work on it at the same time because after assigning a task, we have to rearrange the queue
            pthread_mutex_lock(&mutex_output_queue);

            /*
             * VERY IMPORTANT!!!! 
             * Since multiple threads can work on the same output task queue --> there's a chance of 2+ threads passing the above if statement that 
             * checks if output task queue is empty when output task queue only has 1 (or less than the number of threads working on 3rd stage) output task.
             * SO, just to be safe, after the thread getting an access to the lock --> it should double check if output task queue is still contains a output task
             * If you don't do this, segmentation fault occurs because threads will try to access elements from the queue even though it's empty 
             * 
             * This is happening because we have mutex lock inside the if statement of (!output_task_queue.empty()), not outside. I intentionally did this
             * to give priorities to each stage
            */
            if (output_task_queue.empty()){
                //so if queue is empty, unlock the lock and continue to stay within the while loop (put thread in wait state)
                pthread_mutex_unlock(&mutex_output_queue);
                continue;
            }

            //read the first task in output queue, and remove it afterward
            OutputTask output_task = output_task_queue.front();
            output_task_queue.pop();
            
            //unlock so that other threads can now work on it
            pthread_mutex_unlock(&mutex_output_queue);

            //execute operation for output_task_queue
            execute_output_task(&output_task);
        }
        /*
         * THIS IF STATEMENT IS TO ASSIGN 2RD STAGE TASKS TO THREADS
         * If the above output task queue is empty --> then threads take a look into date task queue
         * because date task queue has the 2nd highest priority (2nd stage)
         * therefore, if output task queue is empty but date task queue is not, then threads grab works from output task queue 
        */
        else if (!date_task_queue.empty()){

            //Use the lock to prevent other threads to work on it at the same time because after assigning a task, we have to rearrange the queue
            pthread_mutex_lock(&mutex_date_queue);

            /*
             * VERY IMPORTANT!!!! 
             * Since multiple threads can work on the same date task queue --> there's a chance of 2+ threads passing the above if statement that 
             * checks if date task queue is empty when date task queue only has 1 (or less than the number of threads working on 3rd stage) date task.
             * SO, just to be safe, after the thread getting an access to the lock --> it should double check if date task queue is still contains a date task
             * If you don't do this, segmentation fault occurs because threads will try to access elements from the queue even though it's empty 
             * 
             * This is happening because we have mutex lock inside the if statement of (!date_task_queue.empty()), not outside. I intentionally did this
             * to give priorities to each stage
            */
            if (date_task_queue.empty()){
                //so if queue is empty, unlock the lock and continue to stay within the while loop (put thread in wait state)
                pthread_mutex_unlock(&mutex_date_queue);
                continue;
            }

            //read the first element of the date task queue
            DateTask date_task = date_task_queue.front();
            //remove the first element of the date task queue
            date_task_queue.pop();

            //unlock so that other threads can now work on it
            pthread_mutex_unlock(&mutex_date_queue);

            //execution for date_task
            execute_date_task(&date_task);
        }
        /*
         * THIS IF STATEMENT IS TO ASSIGN 1st STAGE TASKS TO THREADS
         * Since I'm using the array like a queue for 1st stage month task, I have to use month_task_count to check if there's a task
         * if month_task_count is > 0, then there's a task
        */
        else if (month_task_count > 0){

            //Use the lock to prevent other threads to work on it at the same time because after assigning a task, we have to rearrange the queue
            pthread_mutex_lock(&mutex_month_task_queue);

            /*
             * VERY IMPORTANT!!!! 
             * Since multiple threads can work on the same task queue --> double check that month_task_count is still > 0 after getting the lock because
             * there's a chance that this queue has been emptied by previous thread that was working on this
             * this is happening because we have mutex lock inside the if statement of (month_task_count > 0), not outside. I intentionally did this
             * to give priorities to each stage
            */
            if (month_task_count <= 0){
                //so if queue is empty, unlock the lock and continue to stay within the while loop (put thread in wait state)
                pthread_mutex_unlock(&mutex_month_task_queue);
                continue;
            }

            //read the first task inside the month task queue
            MonthTask task = month_task_queue[0];

            //since using array is faster than queue when it comes to small size, use array like queue
            //rearrange the task queue because we just read the first task from the array
            for (int i = 0; i < month_task_count - 1; i++){
                month_task_queue[i] = month_task_queue[i + 1];
            }
            month_task_count--;
            
            //unlock so that other threads can now work on it
            pthread_mutex_unlock(&mutex_month_task_queue);

            //execute operation required for task queue
            execute_task(&task);

        }
        //else statement only gets called when all the task queues are empty
        else{
            //if no works are available, then break the while loop and let threads to terminate one by one
            break;
        }
    }
}

int main(){
    
    //read input stream
    //IMPORTANT! I have changed the input text file name as "bigw12a_log.txt" from "bigw12a.log.txt" to make sure that I am giving the file as text file to the program
    //in this way, it is also easier to read the name of the file and figure out what type it is
    ifstream file("bigw12a_log.txt");

    //create output file that I'll be writing all the over-heating and over-cooling time
    ofstream output_file("output_task_parallel.txt");
    //close it to save resource. Only open the output file when writing to it
    output_file.close();

    /*
     ************************************************************************************* 
     * First, read through the file, save text input in vector, and find mean + stdev & mean - stdev
     * Exactly the same as the one in data parallelism!
     **************************************************************************************
    */
   
    //keep a time of when the program starts to calculate the total runtime later
    auto beg = std::chrono::high_resolution_clock::now();
    //when input file is open, read it
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

        //read each line of the input text file
        while(getline(file, line)){
            //skip blank line
            if (line == "")
                continue;

            //using stringstream, separate each string by 3 segments by whitespace
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
            //find month
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
            prev_temp = curr_temp;

            //after skipping anomalies & blank line, save the text input for later use
            text_input.push_back(line);

            //if saved date is different, then date has been changed. Therefore, find average (typical temp) of that month
            if (prev_month != curr_month){
                //check if prev_month is not "". It's "" only when your in the beginning of input file (when there's no prev month)
                //so catch edge case here
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

                    //cout << "prev_month: " << prev_month << " typical: " << typical_temp_per_month[prev_month] << " stdev: " << stdev << "\n"; 
                    stdev_high_per_year[prev_year][prev_month] = typical_temp_per_month[prev_month] + stdev;
                    stdev_low_per_year[prev_year][prev_month] = typical_temp_per_month[prev_month] - stdev;

                    temp_list.clear();
                    typical_temp_per_month.clear();

                    //if the prev month and curr month are different, save indices of when prev month starts and ends
                    month_end_idx = text_input.size() - 2;      //update the end idx for previous month
                    month_task_queue[month_task_count] = MonthTask(month_start_idx, month_end_idx); //assign MonthTask using start and end indices of each month
                    month_task_count++; //count up the number of month tasks of the queue
                    month_start_idx = text_input.size() - 1;    //update the start idx for current month (new month because prev month != curr month)
                }
                prev_month = curr_month;

                //if in different month, then we might be in different year as well
                //if we're in different year or prev_year is not yet initialized
                if (prev_year != curr_year){
                    prev_year = curr_year;
                }
            }

            typical_temp_per_month[curr_month] += curr_temp;    //save to later find the mean
            temp_list.push_back(curr_temp);   //save temp_list because we want to know the number of elements within that month using this
        }

        //------if reached the end of file, save the progress of the current month(last month)-------
        //average
        typical_temp_per_month[prev_month] = typical_temp_per_month[prev_month] / temp_list.size();
        //stdev
        float result_val = 0;
        int n = temp_list.size();
        for (int i = 0; i < n; i++){
            result_val += pow(temp_list[i] - typical_temp_per_month[prev_month], 2);
        }
        float stdev = sqrt(result_val / n);
        //---------------------------------------------------------------------------------

        //calculate mean + stdev, mean - stdev of current month (last month)
        stdev_high_per_year[prev_year][prev_month] = typical_temp_per_month[prev_month] + stdev;
        stdev_low_per_year[prev_year][prev_month] = typical_temp_per_month[prev_month] - stdev;

        //also take account of the last month into data_parallel_idx vector, saving its start and end date
        month_end_idx = text_input.size() - 1;
        month_task_queue[month_task_count] = MonthTask(month_start_idx, month_end_idx);
        month_task_count++;

        file.close();
    }

    /*
     ************************************************************
     * 
     * THREADS ASSIGNED FROM HERE
     * 
     * Create threads and assign tasks to them
     * 
     ************************************************************
    */
    //threads
    pthread_t ids[THREAD_NUM];
    //initialize all the mutex locks because we're gonna use them now
    pthread_mutex_init(&mutex_month_task_queue, NULL);
    pthread_mutex_init(&mutex_date_queue, NULL);
    pthread_mutex_init(&mutex_output_queue, NULL);
    pthread_mutex_init(&mutex_file, NULL);

    //create threads from here
    for (int i = 0; i < THREAD_NUM; i++){
        //read text input that is saved in vector again and check for heating and cooling using multiple threads
        if (pthread_create(&ids[i], NULL, &start_thread, NULL) != 0 ){
            perror("Failed to create threads");
        }
    }

    //join threads to terminate from here
    for (int i = 0; i < THREAD_NUM; i++){
        //wait for threads to terminate
        if (pthread_join(ids[i], NULL) != 0){
            perror("Failed to join the thread");
        }
    }

    //destroy all the mutex locks at the end because we no longer need them
    pthread_mutex_destroy(&mutex_month_task_queue);
    pthread_mutex_destroy(&mutex_date_queue);
    pthread_mutex_destroy(&mutex_output_queue);
    pthread_mutex_destroy(&mutex_file);

    //measure the time
    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - beg).count();

    //append the elapsed_time to an existing file
    ofstream reopen_file("output_task_parallel.txt", ios_base::app);
    if (reopen_file.is_open()){
        //write the elapsed time of task parallism to the output file
        reopen_file << "elapsed time for task parallel: " << elapsed_time << " ms\n";
        reopen_file.close();
    }

    return 0;
}