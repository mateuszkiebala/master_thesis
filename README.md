### Master thesis
#### Title:
A library for implementing minimal parallel algorithms.

#### Author:
Mateusz Kiebala

#### Creating library package
Run `mvn package` in `src` directory.

#### Generating random data
Project is using `https://github.com/confluentinc/avro-random-generator`

- Run
  ```avro-random-generator/./arg -f sequential_algorithms/src/main/java/sequential_algorithms/complex.avsc -o data.avro -b -i 1000```
