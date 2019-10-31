### Master thesis
#### Title:
A library for implementing minimal algorithms.

#### Author:
Mateusz Kiebala

#### Setting up Spark with HDFS
1. Go to `install_scripts/`
2. Set up correct variables in `install.sh` and `settings.sh`
3. Run `source install.sh`

#### Creating package
Run `mvn package` in main directory. Place created jar in `target/scala-2.11/`.

#### Tests:
* Unit tests: `mvn test`
* Distributed tests:
    1. Go to `src/test/distributed`
    2. Check if app package is under `target/scala-2.11/library_2.11-0.1.jar`
    3. Enter `group_by` and run `chmod +x run_group_by.sh && ./run_group_by.sh`
    4. Enter `semi_join` and run `chmod +x run_semi_join.sh && ./run_semi_join.sh`
    5. Enter `group_by` and run `chmod +x run_sliding_aggregation.sh && ./run_sliding_aggregation.sh`
