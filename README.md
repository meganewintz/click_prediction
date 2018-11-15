# Click Prediction

The goal of this project is to predict a click on an advertising according to information about users.
It takes a json file containing users as input data and produces a csv file containing the result of the prediction for a sample of the initial data.
A machine Learning Algorithm is used: Logistic Regression.

There is four steps :
- Clean the input data.
- Build a model with training data.
- Test the model with test data.
- Write the result in a csv file.

This project is written in Scala.


## Operation

The program take a json file as input. By default, it takes the file "Data.json" if no file is precised.
The output is a csv file "part-xxx" created in a folder named prediction in the current folder.

## Documentation

Required: sbt 0.1 and scala must be installed.


## Start the sbt shell

    sbt

## Launch the program

    run name_of_your_json_file
