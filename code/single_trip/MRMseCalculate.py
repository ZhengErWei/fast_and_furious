# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol, RawValueProtocol
from mrjob.step import MRStep
import numpy as np
import csv
import math


class MRMseCalculate(MRJob):
    '''
    Calculates the RMSE(root of mean squared error) of the data predicted by the 
    multiple linear regression model fitted before.
    '''

    INPUT_PROTOCOL = RawValueProtocol
    
    INTERNAL_PROTOCOL = JSONProtocol
    
    OUTPUT_PROTOCOL = RawValueProtocol

    def __init__(self,*args, **kwargs):
        super(MRMseCalculate, self).__init__(*args, **kwargs)
        self.counts = 0
        self.mse = 0

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

    def mapper_mse(self,_,line):
        '''
        Calculates squared error and number of observations for data processed by each mapper
        '''
        betas = np.array([-0.00290415,0.00348077, -0.00210198,0.0005363,0.00019483,-0.00014763,0.00126603])
        row = next(csv.reader([line]))
        y,exo = self.extract_variables(row)
        #if len(exo) != self.dim:
            #raise DimensionMismatchError(self.dim,len(exo))
        #if self.options.bias is "True":
        exo.append(1.0)
        #print("exo:",exo)
        x = np.array(exo)
        #print("x:",x)
        y_pred = np.dot(x, betas)
        #print(self.x_t_x)
        self.mse += (y_pred - y) ** 2

        self.counts += 1


    def mapper_mse_final(self):
        '''
        Transforms numpy arrays squared error and number of observations into json-encodable 
        list format and sends to reducer
        '''
        yield 1,("counts", self.counts)  
        yield 1,("mse", self.mse)

    def reducer_mse(self,key,values):
        '''
        Aggregates results produced by each mapper and obtains mean squred error and observations N
        for all data, then calculate the root of mean squared error (RMSE) of linear regression.
        '''
        MSE = 0
        observations = 0
 
        for val in values:
            if val[0] == "mse":
                MSE += val[1]
            elif val[0]=="counts":
                observations += val[1]

        MSE_LR = math.sqrt(MSE/observations)
        yield None, str(MSE_LR)

    def steps(self):
        '''Defines map-reduce steps '''
        return [MRStep(mapper = self.mapper_mse,
                       mapper_final = self.mapper_mse_final,
                       reducer      = self.reducer_mse)]

if __name__=="__main__":
    MRMseCalculate.run()