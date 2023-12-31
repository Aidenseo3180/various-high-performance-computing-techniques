#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <cmath>

using namespace std;

int main(){
    //Input file to read (Using file version A, the smallest file)
    //IMPORTANT! I have changed the input text file name as "bigw12a_log.txt" from "bigw12a.log.txt" to make sure that I am giving the file as text file to the program
    ifstream file("bigw12a_log.txt");

    /*
    *************************************************
    *
    * Serial Version
    * 
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
    *************************************************
    */
    //save stdev high and low for all months of all years
    //the intent is to save both one stdev high & one stdev low from typical temperature (mean) and depending on the month, 
    //either check one stdev higher or lower to make the process simple
    //I decided to use unoredered map of unordered map to save year and its corresponding month that takes O(1) time complexity to search for the data
    //so it saves one stdev higher & lower for all year, all month
    unordered_map<string, unordered_map<string, float> > stdev_high_per_year;   //{year, {month, mean + stdev}}
    unordered_map<string, unordered_map<string, float> > stdev_low_per_year;    //{year, {month, mean - stdev}}

    //While reading the input file, if the current line is not an anomaly or an empty line, then it will be saved to this vector of string
    //because using random access, it is a lot faster to directly access a vector that stores the text line then re-reading the whole file from the beginning again 
    //so this stores each line of the text file
    /*
    * since the maximum size of the vector of string is at least 2^42 – 1 (2^(64 – size(string))-1 where size of string = 22 per line in input file) 
    * in 64bit OS, I knew for sure that it wouldn’t throw the overflow during the operation. I have already confirmed that storing input text file as 
    * vector of string works even for the biggest 2.6GB file given to the class.
    */
    vector<string> text_input;

    /*
     *************************************************************************************
     *
     *  First, read through the file from the beginning to the end, 
     *  save text input in vector, 
     *  and find mean with one standard deviation higher / lower for each month of each year
     * 
     **************************************************************************************
    */

    //get the start time of this program to determine the total runtime at the end
    auto beg = std::chrono::high_resolution_clock::now();

    //if the input text is open, read it through
    if (file.is_open()){
        //each line of the text file
        string line;
        //variable to temporarily store typical temperature of month per year
        unordered_map<string, float> typical_temp_per_month;   //takes {month, typical temp}

        //set-up the variables that I'll be using to store previous values
        //prev_temp is for detecting anomalies. If current temperature is 2 degrees away from prev_temp, then ignore it
        float prev_temp = 0;
        //prev_month is for detecting when the month changes
        string prev_month = "";
        //prev_year is for detecting when the year changes
        string prev_year = "";
        //temporarily store temperatures of all the days within a specific month and give it to typical_temp_per_month unordered_map in the above
        vector<float> temp_list;
        //read each line of the file
        while(getline(file, line)){
            //skip blank line
            if (line == "")
                continue;

            //using stringstream, I separate the line of string into 3 sections (in array) based on the whitespace
            //so the line gets separated by 3 sections: date, time, and temperature
            //Ex. "06/05/04 01:59:38 67.8" -> date, time, temperature
            stringstream ss(line);
            string word;
            //index to indicate where each section of the line should be stored to an array of string named each_line
            int idx = 0;
            string each_line[3];
            while(getline(ss, word, ' ')){  //here, separate string by whitespace, save to string array
                each_line[idx] = word;
                idx++;
            }

            //find the current year by reading the date section from the above, use substring to read only the year
            string curr_year = each_line[0].substr(6, 2);
            //find month by reading the date section from the above, use substring to read only the month
            //This curr_month will be used as index when saving typical temperature to typical_temp_per_month map
            string curr_month = each_line[0].substr(0, 2);
            //convert to string of temperature that we found from the above into a double, save as current time temperature
            float curr_temp = stod(each_line[2]);

            //NOTE assume these months are the months that really don't need any heating and cooling (to save time)
            if (curr_month == "03" || curr_month == "04" || curr_month == "09"){
                //set prev temp to 0 until heating & cooling months start
                prev_temp = 0;
                //just keep skipping lines until we meet heating & cooling months
                continue;
            }

            //check for any anomalies by checking more than 2 degrees away from prev_temp. If it does, skip
            //I placed prev_temp = curr_temp after this if statement so that prev_temp stays the same when anomaly happens
            if (prev_temp + 2 < curr_temp || prev_temp - 2 > curr_temp){
                //if prev_temp is not 0, that means the current month is NOT the beginning of the month
                //since prev_temp only becomes 0 only when while loop start or start of each month after 03, 04, 09 months are passed
                //we can use prev_temp to determine when to skip & not skip when anomalies exist -> if prev_temp is not 0, then we know that 
                //we're either in the beginning of first month (when while loop starts), or we're in heating or cooling months after 03, 04, or 09
                //so we skip if prev_temp is not 0 and exists
                if (prev_temp != 0)
                    continue;
            }

            /*
            *************************************************************************
            * If all the above if statements pass, then we start doing calculations
            *************************************************************************
            */

            //save previous temperature as current temperature
            //we now set this because we know that current temperature is valid
            prev_temp = curr_temp;

            //after skipping anomalies & blank line, save the text input for later use
            //because now we know that this line is valid & useful
            text_input.push_back(line);

            //if saved date is different, then the month has been changed
            //when you find out that you're in different month, calculate average (typical temp) & standard deviation of prev month
            if (prev_month != curr_month){
                //check to make sure that prev_month really existed because we're saving the previous month
                if (prev_month != ""){
                    /*
                    * ************************
                    * Find standard deviation
                    * ************************
                    */
                    //average or typical temperature
                    typical_temp_per_month[prev_month] = typical_temp_per_month[prev_month] / temp_list.size();

                    //formula to find stdev
                    float result_val = 0;
                    int n = temp_list.size();
                    for (int i = 0; i < n; i++){
                        //(current temp - average)^2
                        result_val += pow(temp_list[i] - typical_temp_per_month[prev_month], 2);
                    }
                    //divide above result by n, and square root it to find stdev
                    float stdev = sqrt(result_val / n);
                    //--------------- End of stdev calculation -----------------

                    //Save one stdev higher & one stdev lower for each year, each month
                    stdev_high_per_year[prev_year][prev_month] = typical_temp_per_month[prev_month] + stdev;
                    stdev_low_per_year[prev_year][prev_month] = typical_temp_per_month[prev_month] - stdev;

                    //after calculation, clear temporary variables for future use
                    temp_list.clear();
                    typical_temp_per_month.clear();
                }

                //set prev month to be current month
                prev_month = curr_month;

                //if in different month, then we might be in different year as well
                //if we're in different year or prev_year is not yet initialized
                if (prev_year != curr_year){
                    prev_year = curr_year;
                }
            }

            //As long as we don't move to next month, constantly add current temperature to typical_temp_per_month of current month idx
            //so that we can find the mean later
            typical_temp_per_month[curr_month] += curr_temp;
            //save temp_list because we want to know the number of valid elements within that month using this
            //this will also be used when finding stdev
            //since we already removed anomalies & new lines, this vector will be used to determine the total # of valid temp. within 1 month
            temp_list.push_back(curr_temp);
        }

        /*
        *************************************************************************
        * Since I save each month when the month changes --> if reached the end of file, I have to save the progress of the current month(last month of input file)
        * Process isthe same as what I did in the above (calculating average with stdev)
        *************************************************************************
        */
        //average
        typical_temp_per_month[prev_month] = typical_temp_per_month[prev_month] / temp_list.size();
        //find stdev just like what I did above inside the while loop
        float result_val = 0;
        int n = temp_list.size();
        for (int i = 0; i < n; i++){
            result_val += pow(temp_list[i] - typical_temp_per_month[prev_month], 2);
        }
        float stdev = sqrt(result_val / n);

        //save average + stdev & average - stdev at the same time
        stdev_high_per_year[prev_year][prev_month] = typical_temp_per_month[prev_month] + stdev;
        stdev_low_per_year[prev_year][prev_month] = typical_temp_per_month[prev_month] - stdev;

        typical_temp_per_month.clear();
        //close the input file
        file.close();
    }

    /*
     *************************************************************************************
     *
     * Now read text input that is saved in vector of string
     * because I'm using random access, it is a lot faster to directly access a vector that stores
     * the text line then re-reading the whole file from the beginning again 
     * 
     * and check for over-heating or over-cooling depending on which month we're at
     * 
     *************************************************************************************
    */

    string prev_hour = "";
    vector<string> res;
    //flag that determines when to skip to next hour. 1 = skip until next hour is found, 0 = do not skip and keep going
    //Will use to skip through hours if heating or cooling has been found within that hour
    bool skip_flag = false; 
    for (int i = 0; i < text_input.size(); i++){
        //using random access, retrieve each line more quickly
        //I thought this way would be faster than reading the input file again from beginning because this vector has all the valid temperatures
        string line = text_input[i];

        //split by whitespace using stringstream
        //now separated into 3 segments: date, time, and temperature
        //and these are assigned to string array called "each_line"
        stringstream ss(line);
        string word;
        int idx = 0;
        string each_line[3];
        while(getline(ss, word, ' ')){  //separate the line into 3 segments using spaces
            each_line[idx] = word;
            idx++;
        }

        //get the current year
        string curr_year = each_line[0].substr(6, 2);
        //get the current month
        string curr_month = each_line[0].substr(0, 2);
        //get the current hour
        string curr_hour = each_line[1].substr(0, 2);
        //get current temperature
        float curr_temp = stod(each_line[2]);

        //if current hour is different than previous hour, that means an hour has passed
        //so turn off skip flag because skip flag is used to indicate if we should skip that hour or not
        //when over-heating or over-cooling hour has been found --> then we want to skip to next hour. That's when this flag is used
        //so when this flag is on, we skip rest of seconds within that hour until we're at next hour
        if (prev_hour != curr_hour){
            skip_flag = false;
            prev_hour = curr_hour;
        }

        //skip flag is on which tells the program to skip if heating or cooling time already found within the same hour
        if (skip_flag == true){
            continue;
        }

        //if current month is May to August (cooling months)
        if (curr_month == "05" || curr_month == "06" || curr_month == "07" || curr_month == "08"){
            //check for hours when too much cooling going on
            //go into stdev_low_per_year where we saved average - stdev, find it using current year and current month as indices
            //if current temperature is lower than that --> over-cooling hour has been found & skip until next hour is found (by setting skip flag on)
            if (curr_temp < stdev_low_per_year[curr_year][curr_month]){
                res.push_back(line + " - temp too cold, one stdev lower: " + to_string(stdev_low_per_year[curr_year][curr_month]));
                skip_flag = true;
            }
        }
        //if current month is October to February (heating months)
        else if (curr_month == "10" || curr_month == "11" || curr_month == "12" || curr_month == "01" || curr_month == "02"){
            //check for hours when too much heating going on
            //go into stdev_high_per_year where we saved average + stdev, find it using current year and current month as indices
            //if current temperature is higher than that --> over-heating hour has been found & skip until next hour is found (by setting skip flag on)

            if (curr_temp > stdev_high_per_year[curr_year][curr_month]){
                res.push_back(line + " - temp too warm, one stdev higher: " + to_string(stdev_high_per_year[curr_year][curr_month]));
                skip_flag = true;
            }
        }
    }

    //measure the time by collecting the end time of the program
    auto end = std::chrono::high_resolution_clock::now();
    //find elapsed time by end - start in milliseconds (ms)
    auto elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - beg).count();

    //create output file that I'll be writing all the over-heating and over-cooling time
    string st;
    ofstream output_file("output_serial.txt");
    if (output_file.is_open()){
        //since we've saved all the over-heating & over-cooling hours in res, read the res vector and write to output file
        for (int i = 0; i < res.size(); i++){
            output_file << res[i] << "\n";
        }
        //write the elapsed time to the output file as well
        output_file << "elapsed time for serial version: " << elapsed_time << " ms\n";
        //close the output file
        output_file.close();
    }

    return 0;
}