from Utilities.utils import read_data, get_spark_session, load_config, save_results
from src.analysis import *

class CrashAnalysis:
    def __init__(self):
        self.spark = get_spark_session("DECaseStudy")
        self.input_config = load_config()
        self.load_dataframes()

    def load_dataframes(self):
        self.charges_df = read_data(self.spark, self.input_config['input_files']['charges_path'])
        self.damages_df = read_data(self.spark, self.input_config['input_files']['damages_path'])
        self.endorse_df = read_data(self.spark, self.input_config['input_files']['endorse_path'])
        self.primary_person_df = read_data(self.spark, self.input_config['input_files']['primary_person_path'])
        self.restrict_df = read_data(self.spark, self.input_config['input_files']['restrict_path'])
        self.units_df = read_data(self.spark, self.input_config['input_files']['units_path'])

    def run_analysis(self):
        results = {}

        question1 = self.input_config['questions']['analysis1']
        res1 = analysis_1(self.primary_person_df)
        results['analysis1'] = {'question': question1, 'result': res1}

        question2 = self.input_config['questions']['analysis2']
        res2 = analysis_2(self.units_df)
        results['analysis2'] = {'question': question2, 'result': res2}

        question3 = self.input_config['questions']['analysis3']
        res3 = analysis_3(self.primary_person_df, self.units_df)
        results['analysis3'] = {'question': question3, 'result': res3}

        question4 = self.input_config['questions']['analysis4']
        res4 = analysis_4(self.primary_person_df, self.units_df)
        results['analysis4'] = {'question': question4, 'result': res4}

        question5 = self.input_config['questions']['analysis5']
        res5 = analysis_5(self.primary_person_df)
        results['analysis5'] = {'question': question5, 'result': res5}

        question6 = self.input_config['questions']['analysis6']
        res6 = analysis_6(self.units_df)
        results['analysis6'] = {'question': question6, 'result': res6}

        question7 = self.input_config['questions']['analysis7']
        res7 = analysis_7(self.primary_person_df, self.units_df)
        results['analysis7'] = {'question': question7, 'result': res7}

        question8 = self.input_config['questions']['analysis8']
        res8 = analysis_8(self.primary_person_df)
        results['analysis8'] = {'question': question8, 'result': res8}

        question9 = self.input_config['questions']['analysis9']
        res9 = analysis_9(self.units_df, self.damages_df)
        results['analysis9'] = {'question': question9, 'result': res9}

        question10 = self.input_config['questions']['analysis10']
        res10 = analysis_10(self.primary_person_df, self.units_df, self.charges_df)
        results['analysis10'] = {'question': question10, 'result': res10}

        save_results(results)

if __name__ == "__main__":
    analysis = CrashAnalysis()
    analysis.run_analysis()
