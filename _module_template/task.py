import os
from datetime import datetime
from tempfile import NamedTemporaryFile

from bson import json_util
import json
import sys

from project_template._module_template.align_job import MRJob_template

from module_python2 import BeeModule2


class TemplateModule(BeeModule2):#BeeModule3:
    def __init__(self):
        super(TemplateModule, self).__init__("template")
        #delete hdfs directory found in config path on finish
        self.context.add_clean_hdfs_file(self.config['paths']['all'])

    def launcher_hadoop_job(self, type, input, company=None, devices=None, stations=None, map_tasks=8, red_tasks=8):
        """Runs the Hadoop job uploading task configuration"""
        # create report to save on completion or error
        report = {
            'started_at': datetime.now(),
            'state': 'launched',
            'input': input
        }

        # Create temporary file to upload with json extension to identify it in HDFS
        job_extra_config = self.config.copy()
        job_extra_config.update({#extra_config
                                })
        f = NamedTemporaryFile(delete=False, suffix='.json')
        f.write(json.dumps(job_extra_config))
        f.close()
        report['config_temp_file'] = f.name
        self.logger.debug('Created temporary config file to upload into hadoop and read from job: %s' % f.name)
        # create hadoop job instance adding file location to be uploaded 
        # add the -c configuration file
        mr_job = MRJob_template(
            args=['-r', 'hadoop', 'hdfs://' + input, '--file', f.name, '-c', 'project_template/_module_template/mrjob.conf',
                  '--jobconf', 'mapred.map.tasks=%s' % map_tasks, '--jobconf', 'mapred.reduce.tasks=%s' % red_tasks])
        # mr_job = MRJob_align(args=['-r', 'hadoop', 'hdfs://'+input, '--file', f.name, '--output-dir', '/tmp/prova_dani', '--python-archive', path.dirname(lib.__file__)])  # debugger
        with mr_job.make_runner() as runner:
            try:
                runner.run()
            except Exception as e:
                f.unlink(f.name)
                raise Exception('Error running MRJob process using hadoop: {}'.format(e))

        f.unlink(f.name)
        self.logger.debug('Temporary config file uploaded has been deleted from FileSystem')

        report['finished_at'] = datetime.now()
        report['state'] = 'finished'

        return report


    def module_task(self, params):
        self.logger.info('Starting template module...')
        """CHECK INCONSISTENCIES IN params"""
        try:
            pass
        except KeyError as e:
            raise Exception('Not enough parameters provided to module: {}'.format(e))


if __name__ == "__main__":
    commandDictionary = json.loads(sys.argv[1], object_hook=json_util.object_hook)
    job = TemplateModule()
    job.run(commandDictionary)


    """
from module_edinet.edinet_baseline.task import TemplateModule
from datetime import datetime
params = {
}
t = TemplateModule()
t.run(params) 
    """