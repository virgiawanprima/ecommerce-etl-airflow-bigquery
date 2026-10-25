from pathlib import Path
import importlib.util
import sys
import unittest
import pandas as pd

path=Path(__file__).resolve().parents[1]/'src/utils/qdc.py'
spec=importlib.util.spec_from_file_location('qdc_boundary_target',path)
qdc=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=qdc
spec.loader.exec_module(qdc)

class QDCBoundaryTests(unittest.TestCase):
    def test_exactly_95_percent_is_accepted(self):
        result=qdc.QDCResult('test',100,95,5)
        self.assertTrue(result.meets_target)
        self.assertFalse(result.triggers_fail_fast)

    def test_below_95_percent_is_rejected(self):
        result=qdc.QDCResult('test',100,94,6)
        self.assertFalse(result.meets_target)
        self.assertTrue(result.triggers_fail_fast)

    def test_above_95_percent_is_accepted(self):
        result=qdc.QDCResult('test',100,96,4)
        self.assertTrue(result.meets_target)
        self.assertFalse(result.triggers_fail_fast)

    def test_empty_table_does_not_trigger_failure(self):
        result=qdc.QDCResult('test',0,0,0)
        self.assertEqual(result.success_rate,1.0)
        self.assertTrue(result.meets_target)
        self.assertFalse(result.triggers_fail_fast)

    def test_reported_noise_cases_keep_their_decisions(self):
        for passed,failed,expected in [(112650,0,True),(110398,2252,True),(103638,9012,False)]:
            with self.subTest(failed=failed):
                result=qdc.QDCResult('fact_sales',112650,passed,failed)
                self.assertEqual(result.meets_target,expected)
                self.assertEqual(result.triggers_fail_fast,not expected)

    def test_exact_boundary_for_small_and_large_counts(self):
        for scale in [1,5,1000,10**18]:
            result=qdc.QDCResult('test',20*scale,19*scale,scale)
            self.assertTrue(result.meets_target)
            self.assertFalse(result.triggers_fail_fast)

    def test_integer_comparisons_agree_over_125750_cases(self):
        count=0
        for total in range(1,501):
            for passed in range(total+1):
                failed=total-passed
                result=qdc.QDCResult('test',total,passed,failed)
                self.assertEqual(result.meets_target,100*passed>=95*total)
                self.assertEqual(result.triggers_fail_fast,100*failed>5*total)
                self.assertEqual(result.meets_target,not result.triggers_fail_fast)
                count+=1
        self.assertEqual(count,125750)

    def test_dataframe_pipeline_4_5_and_6_percent_failures(self):
        for bad_rows in [4,5,6]:
            with self.subTest(bad_rows=bad_rows):
                df=pd.DataFrame({'id':range(100),'price':[10.]*(100-bad_rows)+[-1.]*bad_rows})
                passed,failed,result=qdc.run_qdc(df,'test',['id','price'],['id'],{'price':'float64'},{'price':lambda s:s>=0})
                self.assertEqual(len(passed),100-bad_rows)
                self.assertEqual(len(failed),bad_rows)
                self.assertEqual(result.meets_target,bad_rows<=5)
                self.assertEqual(result.triggers_fail_fast,bad_rows>5)

if __name__=='__main__':unittest.main(verbosity=2)
