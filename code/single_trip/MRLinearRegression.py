# Reference: (https://github.com/AmazaspShumik/MapReduce-Machine-Learning/blob/
#            master/Linear%20Regression%20MapReduce/LinearRegressionTS.py)

# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol, RawValueProtocol
from mrjob.step import MRStep
import numpy as np
import csv


# Helper Function -- Cholesky decomposition, which is used to get the coefficients of 
# multiple linear regression

def cholesky_decomposition(x_t_x,x_t_y):
    '''
    Finds parameters of regression through Cholesky decomposition,
    given sample covariance of explanatory variables and covariance 
    between explanatory variable and dependent variable.
    
    Inputs:
    x_t_x : numpy array of size 'm x m', which is the sample covariance of explanatory 
            variables
    x_t_y : numpy array of size 'm x 1', which is the covariance between expalanatory and 
            dependent variable
    
    Return:
    beta : list of size m, which is the values of coefficients 
    '''
    # L*L.T*Theta = x_t_y
    L = np.linalg.cholesky(x_t_x)
    #  solve L*z = x_t_y
    A = np.linalg.solve(L,x_t_y)
    #  solve L.T*Theta = A
    beta = np.linalg.solve(np.transpose(L),A)

    return beta


# Mapreduce Job

class MRLinearRegression(MRJob):
    '''
    Calculates sample covariance matix of explanatory variables (x_t_x) and 
    vector of covariances between dependent variable expanatory variables (x_t_y)
    in single map reduce pass and then uses cholesky decomposition to
    obtain values of regression parameters.	
    '''

    INPUT_PROTOCOL = RawValueProtocol
    
    INTERNAL_PROTOCOL = JSONProtocol
    
    OUTPUT_PROTOCOL = RawValueProtocol

    def __init__(self,*args, **kwargs):
        super(MRLinearRegression, self).__init__(*args, **kwargs)
        n = 7
        self.x_t_x  = np.zeros([n,n])
        self.x_t_y  = np.zeros(n)
        self.counts = 0

    # feature extraction #

    def clean_raw_ind_data(self, row):
        '''
        The function is used to clean the raw index and return a list of explanatory
        variables and y variable

        Input:
        row: each row in a csv file

        Return:
        data: a list of variables 
        '''

        ind_1 = row[1]
        ind_2 = row[2]
        ind_3 = row[3]
        ind_4 = row[4]
        ind_5 = row[5]
        ind_6 = row[6]
        y = row[7]
        data = [float(ind_1), float(ind_2), float(ind_3), float(ind_4), float(ind_5), float(ind_6), float(y)]

        return data
        
    def extract_variables(self,line):
        ''' 
        Call the clean_raw_ind_data function to extracts set of relevant features. 
        '''
        data = self.clean_raw_ind_data(line)
        y,exo = data[6],data[:6]
        return (y,exo)


    #Mapreduce steps #

    def mapper_linear(self,_,line):
        '''
        Calculates x_t_x and x_t_y for data processed by each mapper
        '''
        row = next(csv.reader([line]))
        y,exo = self.extract_variables(row)

        exo.append(1.0)

        x = np.array(exo)

        self.x_t_x  += np.outer(x, x)
        self.x_t_y  += y*x
        self.counts += 1

    def mapper_linear_final(self):
        '''
        Transforms numpy arrays x_t_x and x_t_y into json-encodable list format
        and sends to reducer
        '''
        yield 1,("x_t_x",  [list(row) for row in self.x_t_x])
        yield 1,("x_t_y",  [xy for xy in self.x_t_y])
        yield 1,("counts", self.counts)

    def reducer_linear(self,key,values):
        '''
        Aggregates results produced by each mapper and obtains x_t_x and x_t_y
        for all data, then using cholesky decomposition obtains parameters of 
        linear regression.
        '''
        n = 7
        observations = 0
        x_t_x = np.zeros([n,n])
        x_t_y = np.zeros(n) 
        for val in values:
            if val[0] == "x_t_x":
                x_t_x += np.array(val[1])
            elif val[0] == "x_t_y":
                x_t_y += np.array(val[1])
            elif val[0]=="counts":
                observations += val[1]

        betas = cholesky_decomposition(x_t_x,x_t_y)

        yield None, str(betas)

    def steps(self):
        '''Defines map-reduce steps '''
        return [MRStep(mapper = self.mapper_linear,
                       mapper_final = self.mapper_linear_final,
                       reducer      = self.reducer_linear)]

if __name__=="__main__":
    MRLinearRegression.run()
