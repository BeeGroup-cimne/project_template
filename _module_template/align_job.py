from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol

class MRJob_template(MRJob):
     
    INTERNAL_PROTOCOL = PickleProtocol
    
    def mapper_init(self):
        pass
    def reducer_init(self):
        pass
        
    def mapper(self, _, doc):
        pass
    
    def reducer(self, key, values):
        pass

if __name__ == '__main__':
    MRJob_template.run()