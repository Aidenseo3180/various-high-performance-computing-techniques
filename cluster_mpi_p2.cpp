#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <cmath>
#include "mpi.h"

using namespace std;

const string input_filename = "bigw12a.log.txt";
const string output_filename = "output_loosely_coupled_mpi.txt";

// the number of prev days & next days we want to compare our current day with
// ex. if days_compared = 7, we check previous 7 days and next 7 days from the current day
// I decided to check prev 7 days and next 7 days because those are the days that are most likely going to have the highest similarity
const int days_compared = 7;

/**
 *
 * Overview of steps that I will be taking:
 *
 * 1. open file -> read the whole file and save into map by year-month-day with occurence of temperatures within that day
 *      - while reading file, also save each date using DateInfo struct to the list -> bc we need to know what days are in the map from the above
 * 2. Then, read this map using the DateInfo list -> for each day, compare prev 7 days & next 7 days from that day -> find the most similar day
 *      - then save this most similar day with the cosine similarity value to the vector so that we can write to the output file later on
 * 3. At the end, write the day, most similar day, and their similarity to the output file
 *
 */

/**
 *
 * I created DateInfo struct to store the data in my favor:
 *      - Variable:
 *          - stores year, month, and the day. Day will be different for all the DateInfo objects, but month and year can be the same
 *              - because I'm using DateInfo to basically store each day but since we still need to know which year and month it's in, I decided to design this way
 *      - Methods:
 *          - constructors to instantiate the struct because it must have it
 *          - It has operator overloading == because I am going to compare two DataInfo objects later on
 *          - I also created print_struct method to print the year, month, and day information of that struct to both the output file and to the console
 *
 * Since year, month, and day will be 4 bytes, 2 bytes, and 2 bytes each -> each struct will roughly occupy 8 bytes of memory
 *      - because year will always have 4 characters, month have 2 characters, and day have 2 characters
 *      - (i believe methods are saved in 1 place in memory and each instance has pointer to those methods to save memory, so didn't consider it)
 *
 */
struct DateInfo
{
  string year, month, day;
  DateInfo() {}
  DateInfo(string year, string month, string day)
  {
    this->year = year;
    this->month = month;
    this->day = day;
  }
  bool operator==(const DateInfo &a)
  {
    return (year == a.year && month == a.month && day == a.day);
  }
  string print_struct()
  {
    return " month-" + month + " day-" + day + " year-" + year;
  }
};

/**
 *
 * Global Variables
 *
 * NOTE:
 * log_info_map: stores the feature vector (unordered_map<int,int>) of each day
 *      - this feature vector will have temperatue as key, the occurence or count of that temperature as value
 *      - {each year, {each month, {each day, {feature vector with key-temp and val-count}}}}
 *      - {2010, {10, {29, {72=1, 73=2, 74=1}}}}
 *
 * date_list: list of dates that exist in text file
 *      - Since we do not know what year, what month, and what year we have found from the text file, I made a list to store them
 *      - Again, DateInfo is a struct that stores a specific year, month, and day
 *      - As I read the input file, I use thi date_list to store each day -> later use this to find out which year, month, and day I should access the log_info_map
 *
 * date_list_idx: index that I use to store element to the vector wit random access
 *      - In addition to push_back, since I do not want to add duplicate days (same days), I use this idx to check the last element of date_list
 *      - if current day is the same as element at the end of date_list -> then I skip and don't push the current day
 *
 * average_similarity: global variable for average similarity so that we can calculate it from any of the threads
 *      - since it's getting shared, we need to use mutex lock so that only one thread can make changes to this at a time
 * 
 * write_to_output_file: vector that gets updated by coordinator, it's used to write to output file after collecting outcomes from other processors.
 *
 */
unordered_map<string, unordered_map<string, unordered_map<string, unordered_map<int, int>>>> log_info_map;

vector<DateInfo> date_list;
int date_list_idx = 0;

double average_similarity = 0;

vector<string> write_to_output_file; // vector that gets updated by coordinator, write to output file later

int main(void)
{

  /**
   * 
   * Start of MPI: the entire code runs in each machine
   * 
   * Set up the mpi environment, each machine begins to read the entire input file
   * 
   * Also, begin the timer so that coordinator can calculate the total elapsed time at the end
   * 
  */
  MPI_Init(NULL, NULL);
  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

  ifstream file(input_filename);
  auto start = std::chrono::high_resolution_clock::now();

  /**
   *
   * 1st step: Open the input text file and keep a track of the occurence of temperatures for each day
   *
   */
  if (file.is_open())
  {
    /**
     * 
     * Read each line of the text file
     * 
    */
    string line;
    while (getline(file, line))
    {
      // skip blank line
      if (line == "")
        continue;

      /*
       *
       * Using stringstream, I separate the string by their white spaces -> split them into 3 segments
       * then I store each segment into each_line array
       * Ex. 06/05/04 01:59:37 68.1 -> each_line[0] = 06/05/04, each_line[1] = 01:59:37, each_line[2] = 68.1
       *
       */
      stringstream ss(line);
      string word;
      int idx = 0;
      string each_line[3];
      while (getline(ss, word, ' '))
      {
        each_line[idx] = word;
        idx++;
      }

      /**
       *
       * By reading the substring of each index of the array, I can get current year, current month, current day, and current temperature
       * Since each_line[0] is like 06/05/04 -> can get year, month, day
       * Since each_line[2] only contains temperature as string -> convert to decimal & round it to an int (to make everyone's life easier)
       *
       */
      string curr_year = each_line[0].substr(6, 2);
      string curr_month = each_line[0].substr(0, 2);
      string curr_day = each_line[0].substr(3, 2);
      int curr_temp = round(stod(each_line[2]));

      /**
       *
       * use global variable log_info_map to insert data
       * One thing that I like about map is that if it doesn't contain the key that you're looking for, it creates one for you (only when you
       * try to access them using brackets [])
       * Therefore, if current year, current month, current day, and current temperature doesn't exist in map -> it creates one for you, set it to value 0
       * But since we're currently at current temperature -> add 1 to it (because we want feature vector that stores the occurrence of temperature
       * within that day)
       * In this way, I can keep adding 1s to the corresponding temperature whenever I read the input text file
       *
       */
      log_info_map[curr_year][curr_month][curr_day][curr_temp] += 1;

      /**
       *
       * Now, one problem that we're having here is that we do not know which year,month,or day we have visited so far!
       * So, we need a list that stores all the date that we have found
       *
       * First, make DateInfo with current year, current month, and current day because that's what date_list is made up of.
       * Then, using if statement, first check if date_list is empty -> if it is, we insert the current DateInfo instance
       *      - Have to do this because if date_list is empty but we try to access it using [] -> error occurs
       * If date_list is not empty -> then check if the latest DateInfo element of the list is the same as our current DateInfo instance
       *      - it's only same if we're in same day! This is why I made operator overloading in struct
       *      - if they're not the same -> insert the DateInfo to the list & increment the index by 1 because we have added an element to the list
       *      - Notice that I didn't increment date_list_idx from date_list.empty() if statement. It is because date_list_idx starts from 0
       *          - so when an element is added for the first time to list -> it will be stored at list[0] -> so no need to increment date_list_idx
       *
       */
      DateInfo di = DateInfo(curr_year, curr_month, curr_day);

      if (date_list.empty())
      {
        date_list.push_back(di);
      }
      else if (!(date_list[date_list_idx] == di))
      {
        date_list.push_back(di);
        date_list_idx += 1;
      }
    }

    file.close();
  }

  /**
   * 
   * IMPORTANT:
   * Since each processor is reading the same file, to prevent processors from progressing faster than coordinator, make sure that all the processors
   * are starting from the same place at the same time by giving the barrier after finish reading the input file
   * 
   * If barrier is not given, the program doesn't work as intended because some processors begin to do the work before coordinator
   * even send them the resources that they would need
   * 
  */
  MPI_Barrier(MPI_COMM_WORLD);

  /**
   * 
   * LOAD BALANCING
   * 
   * Now, making sure that all the processors get the work to do is very important. So, I decided to divide date_list_idx, or the total
   * number of days within date_list by the number of tasks. By doing so, I can somewhat evenly distribute task_size length of individual to each processor (from date_list)
   * Again, date_list is a list where all the days are stored. By reading certain index, we can read corresponding day
   * Ex. Index 3 is June 28th, 2004 or something like that
   * 
   * Obviously, since the work is not precisely splitted among processors because I am simply dividing integer called date_list_idx by another integer called num_tasks (or number of machines), there are remaining works at the end.
   *    - Ex. If date_list_idx is 10 and num_tasks is 4 -> 10/4 is 2.5. Since int / int leads to another int, 2.5 gets rounded down to 2. Therefore, 4 machines (processors) get 2 days each.
   *    - In this case, only 8 out of 10 days get processed, so remaining work becomes 2 days
   * These remainding works get handled by the last processor (processor with rank 55 in CRC when core is 2). In this way, I can make sure that all the processors
   * get the work and even if there are some leftovers, the last processor takes care of them.
   * 
   * num_tasks: number of MPI processes
   * 
   * task_size: how each task (or days) should be splitted. All the processors except for the last one will take task_size amount of work
   * 
   * remaining_size: remaining work after all the other processors except for the last one get their work. Last processors know how much work is left
   * based on this value
   * 
   * WARNING: I threw a lot of maths all over the place, so it may be difficult to follow up with how I'm dealing with these indices and data (simply because it's not easy to explain)
   * 
  */
  int num_tasks;
  MPI_Comm_size(MPI_COMM_WORLD, &num_tasks);
  int task_size = date_list_idx / num_tasks;

  int remaining_size = 0;

  /**
   * 
   * Coordinator
   * 
   * When rank is 0 -> send the starting idx and end idx of days that each processor should read
   * Using these indices, each processor access the date_list and calculate similarity
   * 
  */
  if (my_rank == 0)
  {
    /**
     * 
     * So, from here, send read start idx and read end idx to each machine
     * 
     * 
    */
    int starting_idx = 0;
    for (int i = 1; i < num_tasks; i++)
    {
      /**
       * 
       * if last machine is available, give all the remaining works to it
       * Since the for loop was originally running from 0 to date_list_idx in serial version, increment date_list_idx by 1 to make sure that it gets
       * included
       * 
       * By remaining_size = date_list_idx + 1 - starting_idx, calculate remaining size to use when receiving back from the machine
       *    - we're gonna use this in the future
       * 
      */
      if (i == num_tasks - 1)
      {
        int values[2] = {starting_idx, date_list_idx + 1};
        cout << "task:" << i << " reads from idx " << values[0] << " to " << values[1] << "\n";
        remaining_size = date_list_idx + 1 - starting_idx; 
        MPI_Send(values, 2, MPI_INT, i, 0, MPI_COMM_WORLD);
      }
      /**
       * 
       * For all the other machines except for the last one
       * 
       * Same thing, except that now the starting_idx and end_idx are assigned by following task_size
       *    - in other words, difference between starting_idx and end_idx is the task_size (so each machine is getting the same task_size length of days as a work)
       * 
      */
      else
      {
        int values[2] = {starting_idx, starting_idx + task_size};
        cout << "task:" << i << " reads from idx " << values[0] << " to " << values[1] << "\n";
        MPI_Send(values, 2, MPI_INT, i, 0, MPI_COMM_WORLD);
        starting_idx += task_size;
      }
    }

    /**
     * 
     * In here, we're going to deal with receiving back from each machine
     * 
    */
    for (int i = 1; i < num_tasks; i++)
    {
      /**
       * 
       * Since the last machine got different number of task, we have to make sure that we're handling it properly
       * 
      */
      if (i == num_tasks - 1)
      {
        /**
         * recv_similarity: array of double that contains all the similarity. Remaining_size is used as the size
         * recv_current_day: array of int that contains the current days in index form (later use this index to find the correponding date from date_list)
         * recv_most_similar_days: array of int that contains most similar days in index form (later use this index to find the correponding date from date_list)
         * average_sim: average similarity of that machine
         *    - Set average_simlarity to it whenver it comes in because it's the same for all the machines
         * 
         * After receiving all the data back from each machine, put them into write_to_output_file
         *    - it will later be read so that we can write to the output file all at once
         * 
         * Since this for-loop runs from 1 to num_tasks and MPI_Recv forces the program to busy wait until the communication is carried out successfully,
         * the coordinator receives the result back from machines with the rank of 1 to the num_tasks - 1 in order -> keeps the coordination of the outcome and what gets written to the output file
         * 
        */
        double recv_similarity[remaining_size];
        int recv_current_day[remaining_size];
        int recv_most_similar_days[remaining_size];
        double average_sim;
        MPI_Recv(recv_similarity, remaining_size, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(recv_current_day, remaining_size, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(recv_most_similar_days, remaining_size, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&average_sim, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        cout << "Coordinator received from processor with rank " << i << ". Below are what it sent back to coordinator\n";
        for (int z = 0; z < remaining_size; z++)
        {
          cout << "current day:" << date_list[recv_current_day[z]].print_struct() << " most similar day:" << date_list[recv_most_similar_days[z]].print_struct() << " similarity:" << recv_similarity[z] << "\n";
          write_to_output_file.push_back("Current day:" + date_list[recv_current_day[z]].print_struct() + " | most similar to:" + date_list[recv_most_similar_days[z]].print_struct() + " | similarity:" + to_string(recv_similarity[z]));
        }

        average_similarity = average_sim;
      }
      /**
       * 
       * When it's not the last machine, then we use task_size for all (again, remaining_size is to determine all the leftover works that last machine handles).
       * Therefore, the rest of the machines don't need to worry about it
       * 
       * It's pretty much the same as above except the size of these arrays are different
       * 
      */
      else
      {
        double recv_similarity[task_size];
        int recv_current_day[task_size];
        int recv_most_similar_days[task_size];
        double average_sim;
        MPI_Recv(recv_similarity, task_size, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(recv_current_day, task_size, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(recv_most_similar_days, task_size, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&average_sim, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        cout << "Coordinator received from processor wth rank " << i << ". Below are what it sent back to coordinator\n";
        for (int z = 0; z < task_size; z++)
        {
          cout << "current day:" << date_list[recv_current_day[z]].print_struct() << " most similar day:" << date_list[recv_most_similar_days[z]].print_struct() << " similarity:" << recv_similarity[z] << "\n";
          write_to_output_file.push_back("Current day:" + date_list[recv_current_day[z]].print_struct() + " | most similar to:" + date_list[recv_most_similar_days[z]].print_struct() + " | similarity:" + to_string(recv_similarity[z]));
        }

        average_similarity = average_sim;
      }
    }

    /**
     * 
     * After finish sending and receiving data back and forth, write to the output file using write_to_output_file
     * 
     * Also write down the elapsed time and average similarity
     * 
     * Then, close the output file.
     * 
     * After this if statement ends, MPI_Finalize() that is located all the way at the bottom gets called and terminate the MPI environment
     * 
    */
    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    ofstream output_file(output_filename);
    if (output_file.is_open())
    {
      for (int i = 0; i < write_to_output_file.size(); i++)
      {
        output_file << write_to_output_file[i] << "\n";
      }
      output_file << "Elapsed time for cluster version:" << elapsed_time << " ms\n";
      cout << "Elapsed time for cluster version:" << elapsed_time << " ms\n";

      output_file << "Average Similarity:" << average_similarity << "\n";
      cout << "Average Similarity:" << average_similarity << "\n";

      output_file.close();
    }
  }
  /**
   * 
   * For each machines, they're going to use given starting_idx and ending_idx (recv[0] and recv[1]) to read a range of days
   * For each day within the range, the machine performs comparison (In this case, it's previous 7 days and next 7 days, 
   * but can be changed by days_compared)
   * 
   * Then, each machine finds the average similarity and most similar day of each day within the range and send them off to coordinator at the end
   * 
   * BASICALLY, from here, each machine reads days within the range of starting idx(recv[0]) and end idx(recv[1]) by using these indices to access date_list, a list that keeps all the existing dates in input file
   * Then, each of these days gets compared with previous days_compared value (prev 7 days), and next days_compared (next 7 days).
   * During the comparison, find the cosine similarity -> find the 2 days that are most similar to one another based on this similarity (also find the average similarity as well)
   * Then, we save these information and send them to the coordinator so that it can write to the output file
   * 
   * 
  */
  else
  {
    /**
     * 
     * recv[2] = starting and ending idx
     * 
     * n = size of the task. If the machine is the last machine -> this n = remaining_size. If not, this n = task_size
     * 
     * send_highest_similarity: store the highest similarity
     * 
     * current_day: store the current day (each day from range of days starting from staring idx to end idx)
     * 
     * send_most_similar_days: store the day that has the highest similarity
     * 
     * The reason why these data are stored like this is because they can easily be access all at once using the same index.
     * For example, if we're at index of 1:
     *    - send_highest_similarity[1] will contain the cosine similarity between send_most_similar_days[1] and current_day[1]
     * 
     * So, by passing the data in this way to coordinator, these arrays can be easily accessed
     * 
    */
    int recv[2];
    MPI_Recv(recv, 2, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    int n = recv[1] - recv[0];
    double send_highest_similarity[n];
    int highest_similarity_cnt = 0;

    // index of current day
    int current_day[n];
    int current_day_cnt = 0;

    // idx of all the days it was compared to
    int send_most_similar_days[n];
    int most_similar_days_cnt = 0;

    /**
     *
     * Step 2: Read the log_info_map that has feature vector (each temperature with occurrences for each day) using date_list and find cosine similarity
     *
     * I decided to compare each day with prev 7 days & next 7 days -> find the day that's most similar to current day based on similarity
     *
     * Since I want to read each DateInfo from date_list -> have a gigantic for-loop that runs from 0 - date_list_idx
     *      - we can do this because date_list_idx is the same as the size of date_list
     *      - And using each DateInfo from date_list -> we access log_info_map by each year -> each month -> each day and retrieve its feature vector
     *
     * NOTE:
     * Since there are multiple parts of cosine similarity, I have splitted up the process into several steps.
     * I have added comments to indiciate which section of code is for which part of cosine similarity
     *
     */
    for (int i = recv[0]; i < recv[1]; i++)
    {

      /**
       * 1st step of cosine similarity: sum of temperatures of current day
       *
       * Since formula of cosine similarity of 2 vectors A and B is:
       *                      sum from i = 0 to N-1 of A * B
       *      ---------------------------------------------------------
       *      (square root of sum of A^2) * (square root of sum of B^2)
       *
       * Where vector A is current day, vector B is the another day that we'll compare with -> I thought it'll be better to do the calculation
       * now for the denominator part of sum of A^2 right now & reuse this for previous 7 days and next 7 days calculations (will explain more in detail)
       * (didn't square root it yet because I didn't feel like it)
       *
       * The for-loop that I used to find the sum only goes 50 - 90 because I already know that the dataset measured the temp from in-door and
       * it never goes below 50 or above 90 (I read through the dataset and didn't find any)
       * So, just to be efficient, I only ran it from 50-90 -> if current_day_temp which is the feature vector of current day contains this
       * specific temperature j -> then I calculate and add it to the current sum
       *      - Formula is pow(j,2) * curr_day_temp[j] because j is temperature, curr_day_temp[j] is its occurrence
       *      - Ex. if j = 70 and curr_day_temp[70] = 3 -> since curr_day_temp[70] is equiv to having three 70 degrees -> can do 70^2 * 3 to find its sum
       *      - Again, we're finding sum of A^2 (each element of A ^ 2) in this step
       *
       */
      unordered_map<int, int> curr_day_temp = log_info_map[date_list[i].year][date_list[i].month][date_list[i].day];
      double curr_sum = 0;

      for (int j = 50; j < 90; j++)
      {
        curr_sum += pow(j, 2) * curr_day_temp[j];
      }

      /**
       *
       * Check previous 7 days, next 7 days
       *
       * Since we want to compare current day with other days -> I chose the previous 7 days and next 7 days from current day to compare with
       *
       * NOTE:
       * similarity_array[days_compared * 2]: array that will store the similarity between current day & previous 7 and next 7 days (days_compared = 7)
       *      - so it has size of 14 (7 previous, 7 next days)
       *      - since we have to keep the other day with its similarity with current day -> use pair to store other days' DateInfo & its similarity
       *          - Again, DateInfo is simply a grouping of specific year, specific month, and specific day
       *      - by default, DateInfo is set using default constructor with 0 similarity to avoid any weird issues that can come from not assigning to 0
       *
       * similarity_cnt: count used to access similarity array & put values into it
       *      - to assign pairs to each similarity_array -> we need to know at which index we want the insert the pair
       *      - so I'm using separate counter to handle that
       *
       * start_idx: starting index to determine how we're going to read previous 7 days to next 7 days
       * end_idx: end index to determine when we have reached the end of next 7 days
       *      - these are based on i, which is an index used to access each date_list because we also want to access the date from date_list at different index
       *      - have to do this bc i - 7 or i + 7 can lead to out-of-range -> so must check beforehand
       *      - For start_idx: check if i - 7 is not < 0
       *          - If it is, set to 0. Otherwise, set to i - 7
       *      - For end_idx: check if i + 7 > size of date list map
       *          - If it exceeds the size, set to date_list_idx. Otherwise, set to i + 7
       *      - If you want to change the range of days that you want to compare to -> then simply change the number 7 to something else
       *
       * I then use start_idx and end_idx to check previous / next 7 days from current day
       *      - if there are no previous 7 days or next 7 days -> it will try to read as much as possible
       *      - Ex. if we're reading the end of date_list -> since it doesn't have next 7 days, it will only read the previous 7 days
       */

      pair<DateInfo, float> similarity_array[days_compared * 2] = {make_pair(DateInfo(), 0)};
      int similarity_cnt = 0;

      int start_idx = (i - days_compared < 0) ? 0 : i - days_compared;
      int end_idx = (i + days_compared > date_list_idx) ? date_list_idx : i + days_compared;

      // run through previous 7 days to next 7 days
      for (int j = start_idx; j <= end_idx; j++)
      {
        // if j == i, skip bc j (the other day) the same as i (current day)
        if (j == i)
          continue;

        // feature vector of the day. Has temperature as key, occurrence as val
        unordered_map<int, int> other_day_temp = log_info_map[date_list[j].year][date_list[j].month][date_list[j].day];

        /**
         *
         * 2nd step of cosine similarity: Find the denominator part of the other day
         *
         * As the formula from the above shows, B is the same as the other day -> so find sum of B^2 (each temperature of the other day^2) from here
         * Then I will use this with sqaure root -> find cosine similarity later on
         *
         * Again, I decided to run from 50 to 90 because I couldn't find any temp that exceeds these 2 points -> so just to efficiently loop through
         *
         */
        double other_sum = 0;
        for (int k = 50; k < 90; k++)
        {
          other_sum += pow(k, 2) * other_day_temp[k];
        }

        /**
         *
         * 3rd step of cosine similarity: Find the numerator of the formula
         *
         * Now, since we have feature vector of current day and feature vector of the other day -> calculate the numerator of the formula
         * Again, I decided to run from 50 to 90 because I couldn't find any temp that exceeds these 2 points -> so just to efficiently loop through
         *      - Again, k is the temperature that we're trying to find from feature maps -> simply doing map[k], we can find its value (the occurrence)
         *
         * First, I made a variable numerator with long long int type because it gets really big as it sums up all the temperature from both current and the other day
         * Then, I made an occurrence variable that basically compares the occurrence of temperature k from both current day's feature map and the other day's feature map
         *      - Since we're looking for cosine similarity: choose the lowest occurrence of the temperature k from the two and use that to calculate how many times we have to add that temperature k!
         *          - That's why I'm using a formula of: occurrence * (k * k)
         *              - k * k is from Ai * Bi, occurrence is how many times both feature vector appear to have the temperature k
         *          - For ex. if k = 70, curr_day_temp[k] = 3, other_day_temp[k] = 5:
         *              - then occurrence would be 3 (bc it's the lowest) and numerator will get added by 3 * (70 * 70) which makes sense because k only appears 3 time in current day
         *              - again, current day: day that we found from for-loop with i (in the very beginning), other day: day that we found from for-loop with start & end index j (previous 7 days to next 7 days)
         *      - I set it to double just to be safe
         *
         */
        long long int numerator = 0;
        for (int k = 50; k < 90; k++)
        {
          double occurence = curr_day_temp[k] <= other_day_temp[k] ? curr_day_temp[k] : other_day_temp[k];
          // then, do k * k (like A * B. k is a temperature btw) and multiply that with occurence, or # where the temp occurs from both days
          numerator += occurence * (k * k);
        }

        /**
         *
         * Last step of cosine similarity: using the variables that we've found so far, calculate similarity
         *
         * Now, we simply square root both current day's sum and other day's sum -> multiply them together for denominator part
         * Then, divide numerator by denominator to find the cosine similarity
         * Then, create a pair that saves the information of the other day into DateInfo with its similarity to the current day -> and save this to similarity_array using similarity_cnt
         *      - again, similarity array is an array with size 6 that we created in the beginning to temperorary store previous 7 days and next 7 days' similarity to the current day
         * Then we have to increment the count by 1 because this count is used to access the index of the array where we save the similarity
         *
         */
        similarity_array[similarity_cnt] = make_pair(DateInfo(date_list[j].year, date_list[j].month, date_list[j].day), numerator / (sqrt(curr_sum) * sqrt(other_sum)));
        similarity_cnt += 1;
      }

      /**
       *
       * Now, run through the similarity_array that has previous 7 days & next 7 day's similarity -> find the one that has the higheset cosine similarity
       *
       * Since I have set similarity array to 0 by default -> if there aren't past 7 days or next 7 days (if either in the near-beginning or near-end of the date list),
       * then the rest of array will stay as 0. But since 0 is lowest similarity compared to other valid days -> doesn't affect our result and will be ignored by below for-loop
       *
       * Since we have to find the highest similarity, I've made max_similarity to temporary store the highest similarity, most_similar_day to store the other day with the highest similarity
       *      - And if there are days with higheset similarity than max_similarity -> set max_similarity to that day's similarity and set most_similar_day to that as well
       *
       * The for-loop runs from 0 to similarity_cnt because similarity_cnt is the total size of an array
       *
       * Also, from here, as we find the most_similar_day, make sure to also save most_similar_day_idx because we need to pass it to coordinator
       * 
       */
      float max_similarity = 0;
      int most_similar_day_idx = 0; // instead of sending the day back to coordinator, send its idx
      DateInfo most_similar_day = DateInfo();
      for (int j = 0; j < similarity_cnt; j++)
      {

        if (max_similarity < similarity_array[j].second)
        {
          most_similar_day = similarity_array[j].first; // first: DateInfo that has year, month, and the day
          max_similarity = similarity_array[j].second;  // second: similarity
          most_similar_day_idx = j;
        }

        /*
         * Since you're checking previous 7 days and next 7 days -> there's a duplicate similarity
         * Ex. let's say that we're comparing prev & past 7 days from 12/20 and prev & past 7 days from 12/21
         * 12/20 will calculate shared similarity with other days from 12/12 - 12/27
         * 12/21 will calculate shared similarity with other days from 12/13 - 12/28
         * Since comparing 12/12 - 12/27 with 12/20 has shared similarity between 12/20 and 12/21 AND comparing 12/13 - 12/28 with 12/21 also has shared similarity between 12/20 and 12/21 
         * -> we shouldn't consider one of them as part of our average simiarity (we don't want to add the same similarity twice)
         *
         * Therefore, we have to take care of it by checking when j is 6. When j=6, it has the shared similarity between current day and previous 1 day
         * So, using the same example, j=6 of 12/21 will be the shared similarity between 12/20 and 12/21
         * So we ignore this by checking when j=6 AND i != 0 (because we want to take account of everything in the very beginning of days
         * Again, the for-loop i runs from 0 to date_list_idx (bc it's the same as the size of date_list that stores all the dates) so when i = 0, that's when
         * we first check this date_list and reading the days from the beginning
         *
        */
        if (i != 0 && j == days_compared - 1)
        { 
          // j==6 (same as days_compared - 1 when days_compared = 7) is same as -1, j==7 is the same as +1 from i
          continue;
        }

        average_similarity += similarity_array[j].second;
      }

      /**
       * 
       * check if i = 0, find out whether the sum should be divided into similarity_cnt or similarity_cnt - 1
       * Again, when i = 0, we added all. But when it's not, we skipped j=6 and added only similarity_cnt - 1 number of elements
       * 
      */
      average_similarity = (i == 0) ? average_similarity / similarity_cnt : average_similarity / (similarity_cnt - 1);

      /**
       *
       * date_list[i] is the current day, most_similar_day is the other day that's most similar to current day
       *      - I used pair to pair up most_similar_day with its similarity so that I can write it to the output file later on.
       *
      */
      send_highest_similarity[highest_similarity_cnt] = max_similarity; // save max similarity
      highest_similarity_cnt += 1;

      current_day[current_day_cnt] = i; // save current day, send the idx for date_list
      current_day_cnt += 1;

      /**
       * 
       * IMPORTANT:
       * Since the for-loop for previous & next days_compared days (7 for each, so this for-loop goes 0 to 14) starts from 0 to days_compared * 2 (same as similarity_cnt),
       * We have to do some math do figure out where the most similar days are located within date_list.
       * Why? because we're sending the index back to the coordinator. Since coordinator will use these indices to find the corresponding most-similar days
       * from date_list -> needs to find out where it's going to be
       * 
       * Why am i sending indices instead of a character array? It is because date_list is already a global variable so it's a lot easier to send an array of int that has all the indices
       * and use them to acces the correponding days from this list.
       * 
       * Therefore, we first have to check whether the day is at the beginning or at the end
       *    - why? Because we checked previous & next 7 days (14 days in total), but if we're at the beginning or at the end, they're going to have less days to be checked
       *    - Ex. if we're at the beginning, we might not be able to read all previous 7 days because they don't exist
       *    - Ex. for the same reason, if we're at the end, we might not be able to read all next 7 days because they don't exist
       * 
       * WARNING: I'm bad at explaining
       * 
      */
      /**
        * 
        * THIS IS FOR AT-THE-BEGINNING CASE
        * 
        * when j < i, most_similar_day_idx can stay the same
        * if not, then idx has to be added by 1
        * ex. If i = 2, then there's 2 prev days and 7 next days.
        * In this case, if most_similar_day_idx is either 0 or 1 -> the same idx can be used to access date_list
        * but if greater than i(2), then if most_similar_day_idx is 2 -> it has to be added by 1 so that 3rd idx of date_list can be accessed
        * 
      */
      if (i < days_compared)
      {
        if (most_similar_day_idx >= i)
        {
          most_similar_day_idx = most_similar_day_idx + 1;
        }
      }
      /**
       * 
       * THIS IS FOR AT-THE-END CASE
       * 
       * When i > date_list_idx - days_compared -> that's when we know that we're not gonna have all next 7 days from current day
       * Therefore, we have to manually calculate the index of the most similar day by doing some math
       * 
       * If most_similar_day_idx >= days_compare:
       *    - Ex. there are total of 348 days in our file. For ex. if we're at 344 (this is i, the current index) and most-similar-idx happened to exist at 8 (whatever we find from j for-loop from above)
       *    - Then this if statement gets called becaue 8 >= 7
       *    - Therefore, most_similar_day_idx = 344 - 7 + 8 + 1 = 346. This makes sense because previous 7 days (equivalent to index 0 - 6).
       *    - So, days after 344 will be index 7,8,9, and 10. And index 8 in this case is equiavalent to index 346 in date_list
       * 
       * Otherwise: don't add 1. Same logic except that the index of the most-similar-day is in the left-side of the current day.
       *    - so we don't have to take account of current day as part of the index (because current day cannot be the most-similar day of itself)
       * 
       * 
      */
      else if (i > date_list_idx - days_compared)
      {
        if (most_similar_day_idx >= days_compared)
        {
          most_similar_day_idx = i - days_compared + most_similar_day_idx + 1;
        }
        else
        {
          most_similar_day_idx = i - days_compared + most_similar_day_idx;
        }
      }
      /***
       * 
       * IF IN THE MIDDLE (meaning days_compared * 2 for similarity_cnt, so now we have full previous 7 days and next 7 days that we compare the current day to)
       * Since most_similar_day_idx can go from 0 to 14 (when days are prev 7 and next 7, can calculate the idx of most similar day in date_list by
       * i + most_similar_day_idx - days_compared
       * 
       * Ex. If current index i = 143 and most_similar_day_idx = 5 when total days are 348:
       *    - 143 + (5 - 7) = 141. This is the index that we use to find the correponding day from date_list
       * 
       * But the index can get messed up when most_similar_day_idx >= days_compared because most_similar_day_idx goes from 0-13, and if it's
       * greater than 7 (for example), we have to increment it by 1 to avoid accessing 1 index behind where we truly are
       * 
      */
      else
      {
        if (most_similar_day_idx >= days_compared){
          most_similar_day_idx = i + (most_similar_day_idx - days_compared) + 1;
        }
        else{
          most_similar_day_idx = i + (most_similar_day_idx - days_compared);
        }
      }

      send_most_similar_days[most_similar_days_cnt] = most_similar_day_idx;
      most_similar_days_cnt += 1;
    }

    /**
     * 
     * After finding all the necessary data, send them back to coordinator
     * The coordinator will use these to fill out the output file later on
     * 
    */
    MPI_Send(send_highest_similarity, n, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
    MPI_Send(current_day, n, MPI_INT, 0, 0, MPI_COMM_WORLD);
    MPI_Send(send_most_similar_days, n, MPI_INT, 0, 0, MPI_COMM_WORLD);
    MPI_Send(&average_similarity, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
  }

  // end the MPI
  MPI_Finalize();

  return 0;
}
