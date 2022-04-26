#!/usr/bin/env python
# coding: utf-8

# In[4]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import csv
import math

class NeuralNetwork(object):

    def __init__(self, inputs, hidden, outputs):

        self.activation = sigmoid
        self.activation_prime = sigmoid_prime

        self.output_act = sigmoid
        self.output_act_prime = sigmoid_prime


        # Weights initializarion
        self.wi = np.random.randn(inputs, hidden)
        self.wo = np.random.randn(hidden, outputs)

        # Weights updates initialization
        self.updatei = 0
        self.updateo = 0


    def feedforward(self, X):

        # Hidden layer activation
        ah = self.activation(np.dot(X, self.wi))

        # Adding bias to the hidden layer results
        #ah = np.concatenate((np.ones(1).T, np.array(ah)))

        # Outputs
        y = self.output_act(np.dot(ah, self.wo))

        # Return the results
        return y

    def mse_loss(self, y_true, y_pred):
        return ((y_true - y_pred) ** 2).mean()


    def fit(self, X, y, epochs=10, learning_rate=0.2):

        # Epochs loop
        for k in range(epochs):

            # Dataset loop
            for i in range(X.shape[0]):

                # Hidden layer activation
                ah = self.activation(np.dot(X[i], self.wi))
                # Output activation
                ao = self.output_act(np.dot(ah, self.wo))

                # Deltas
                deltao = np.multiply(self.output_act_prime(ao),y[i] - ao)
                deltai = np.multiply(self.activation_prime(ah),np.dot(self.wo, deltao))


                # Weights update
                self.updateo = np.multiply(learning_rate, np.outer(ah,deltao))
                self.updatei = np.multiply(learning_rate, np.outer(X[i],deltai))

                self.wo = self.wo + self.updateo
                self.wi = self.wi + self.updatei

      
            loss_output = []
            if epochs % 10 == 0:
                y_preds = np.apply_along_axis(self.feedforward, 1, X)
                loss = self.mse_loss(y, y_preds)
                
                loss_output.append(loss)
                #print(loss_output)


    def predict(self, X):

        y = np.zeros([X.shape[0],self.wo.shape[1]])

        for i in range(0,X.shape[0]):

            y[i] = self.feedforward(X[i])

        return y
    
 




# Activation functions
def sigmoid(x):
    return 1.0/(1.0 + np.exp(-x))

def sigmoid_prime(x):
    return sigmoid(x)*(1.0-sigmoid(x))



def mse_loss(y_true, y_pred):
    return ((y_true - y_pred) ** 2).mean()

header = ['x1','x2','y1','y2']
df = pd.read_csv("training_data.csv", usecols=header)
x1 = df["x1"]
x2 = df["x2"]
y1 = df["y1"]
y2 = df["y2"]
x = np.array([x1,x2]).T
y = np.array([y1,y2]).T

NN = NeuralNetwork(2,4,2)

NN.fit(x,y,epochs=1000,learning_rate=0.1)
with open('predicted.csv', 'w', newline = '') as f:
    writer = csv.writer(f)
    header = ['y1', 'y2' ]
    writer.writerow(header)
    writer.writerows(NN.predict(x))


data = pd.read_csv("predicted.csv", usecols=header)
x1_axis = df["x1"]
y1_axis = data["y1"]




## predicted plots
data = pd.read_csv("predicted.csv", usecols=header)
x1 = df["x1"]
y1 = data["y1"]
x1_axis = [x1]
y1_axis = [y1]

plt.scatter(x1, y1, color = 'g',s = 1)
plt.xlabel('x')
plt.ylabel('y1')
plt.title('y1 predictions')
  
plt.show()

x2 = df["x2"]
y2 = data["y2"]
x2_axis = [x2]
y2_axis = [y2]

plt.scatter(x2, y2, color = 'b',s = 1)
plt.xlabel('x')
plt.ylabel('y2')
plt.title('y2 predictions')
  
plt.show()






