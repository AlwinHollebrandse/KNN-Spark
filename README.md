# ScalaKNN
A KNN classifier written with Scala.
 
## Running:
A terminal with Scala installed is required to run this code. Provided that requirement is met, run `mvn package` in the terminal within the correct maven directory.
`hadoop jar <Jar file maven made> <main file> <training dataset> <output folder> <test dataset> <K>`.
`spark-submit --class <package of main> --master local <Jar file maven made> <training dataset> <test dataset> <K>`

### Sample Input:
`spark-submit --class com.alwin.KNN --master local target/ScalaSparkKnn-0.0.1-SNAPSHOT.jar smallTrain.txt smallTrain.txt 5`

### Sample Output:
Along the traditional Scala output, this program will print something like:
The KNN classifier for 336 instances required 2839 ms CPU time, accuracy was 0.8720238095238095
 
## Implementation Details:
This code works by first creating the `SparkSession` that will perform the KNN job. After writing a helper script to convert all three datasets into `LIBSVM` format, this code would read the train and test datasets that were provided as command line arguments. These datasets were converted into `DataFrames`. These DataFrames were then passed to the nearestKClasses function. This function created a variable for both the feature sets of train and test and the training classifications. This function would then map through the test data and compute the distances of each training point. This was achieved by zipping the features data with the training classes as arrays. The Euclidean distance was computed for each point and tupled together with the training class that had the resulting distance. This class/distance array was then sorted by distance in ascending order. The closest `k` instances were then taken and had their class read from the tuple. The variable `classCounts` would then perform the majority voting of those closest `k` classes. The results of the `findKNearestClasses` were then sent to the `findAccuracy` method along with an array of the actual test classes. This function would zip the provided actual class array with the given prediction array (that came from and the `findKNearestClasses` function). This zip was then filtered to only instances that had the actual class match the prediction class. The size of this filter was divided by the original number of test instances. The final step was to report the computed accuracy of this KNN classifier and the CPU time needed for the classifier to run. The CPU time is computed by using "System.nanoTime()" functionality before and after the setup and the kNN call. A portion of the inspiration source code can be found [here](https://stackoverflow.com/questions/28949591/easiest-way-to-represent-euclidean-distance-in-scala).

## Results:
The following times were computed by averaging the run CPU times (in ms) for each of the provided dataset 3 times with a k of value 5. It should be noted that for this experiment, the test dataset was identical to the training dataset. This was done to match the previous KNN experiments. However, this code has no method to ignore a distance if the instance of the training and test class were identical. This resulted in the closest neighbor for each test point being itself. However, this would never be an issue with a proper train test data split.
 
| dataset: | Time (ms) | Accuracy |
| --- | --- | --- |
| small | 2839 | 0.869047619047619 |
| medium | 17009 | 0.65659452837893 |
| large | 239036 | 1.0 |