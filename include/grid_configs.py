from numpy.random.mtrand import seed
from sklearn.linear_model import LogisticRegression
import lightgbm as lgb



models = {
    'lgbm': lgb.LGBMClassifier(objective='binary', metric=['auc', 'binary_logloss'], seed=55, boosting_type='gbdt'),
    'log_reg': LogisticRegression(max_iter=500)
}

params = {
    'lgbm':{
        'learning_rate': [0.01, .05, .1], 
        'n_estimators': [50, 100, 150],
        'num_leaves': [31, 40, 80],
        'max_depth': [16, 24, 31, 40]
        },
    'log_reg':{
        'penalty': ['l1','l2','elasticnet'],
        'C': [0.001, 0.01, 0.1, 1, 10, 100],
        'solver': ['newton-cg', 'lbfgs', 'liblinear', 'sag', 'saga']
        }
    }