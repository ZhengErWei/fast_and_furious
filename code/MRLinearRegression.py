from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol, RawValueProtocol
from mrjob.step import MRStep
import numpy as np

####################################################################
# Helper Function
####################################################################

def cholesky_decomposition(x_t_x,x_t_y):
    '''
    Finds parameters of regression through Cholesky decomposition,
    given sample covariance of explanatory variables and covariance 
    between explanatory variable and dependent variable.
    
    Inputs:
    -------
    x_t_x : numpy array of size 'm x m', which is the sample covariance of explanatory 
            variables
    x_t_y : numpy array of size 'm x 1', which is the covariance between expalanatory and 
            dependent variable
    
    Output:
    -------
    beta : list of size m, which is the values of coefficients 
    '''
    # L*L.T*Theta = x_t_y
    L = np.linalg.cholesky(x_t_x)
    #  solve L*z = x_t_y
    A = np.linalg.solve(L,x_t_y)
    #  solve L.T*Theta = A
    beta = np.linalg.solve(np.transpose(L),A)

    return beta

####################################################################
# Helper Class
####################################################################

class DimensionMismatchError(Exception):

    def __init__(self, expect_dim, observe_dim):
        self.exp = expect_dim
        self.obs = observe_dim
        
    def __str__(self):
        error = "Expected number of dimensions: "+str(self.exp)+", observed: "+str(self.obs)
        return error


####################################################################
# Mapreduce Job
####################################################################

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
        n = self.options.dimension
        self.x_t_x  = np.zeros([n,n])
        self.x_t_y  = np.zeros(n)
        self.counts = 0

    # feature extraction #
        
    def extract_variables(self,line):
        ''' (str)--([float,float,float...],float)
        Extracts set of relevant features. (Needs to be rewriten depending
        on file input structure) 
        '''
        data = [float(var) for var in line.strip().split(",")]
        y,exo = data[6],data[:6]
        return (y,exo)

    # Options #
        
    def configure_options(self):
        ''' Additional options'''
        super(MRLinearRegression,self).configure_options()
        self.add_passthrough_option("--dimension", 
                                    type = int,
                                    help = "Number of explanatory variables (do not count bias term)")
        self.add_passthrough_option("--bias", 
                                    type = str, 
                                    help = "Bias term, bias not included if anything other than 'True' ",
                                    default = "True")
                                    
    def load_options(self,args):
        ''' Loads and checks whether options are provided'''
        super(MRLinearRegression,self).load_options(args)
        if self.options.dimension is None:
            self.option_parser.error(" define number of explanatory variables")
        else:
            self.dim = self.options.dimension

    # Mapreduce steps #

    def mapper_linear(self,_,line):
        '''
        Calculates x_t_x and x_t_y for data processed by each mapper
        '''
        y,exo = self.extract_variables(line)
        if len(exo) != self.dim:
            raise DimensionMismatchError(self.dim,len(exo))
        if self.options.bias is "True":
            exo.append(1.0)
        x = np.array(features)
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
        n = self.dim
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
        yield None,[beta_i for beta_i in betas]

    def steps(self):
        '''Defines map-reduce steps '''
        return [MRStep(mapper = self.mapper_linear,
                       mapper_final = self.mapper_linear_final,
                       reducer      = self.reducer_linear)]

if __name__=="__main__":
    MRLinearRegression.run()