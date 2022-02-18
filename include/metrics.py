import logging
import matplotlib.pyplot as plt
import mlflow
import numpy as np
from sklearn.metrics import classification_report, confusion_matrix, ConfusionMatrixDisplay, roc_curve, roc_auc_score
import pandas as pd



def log_roc_curve(y_test: list, y_pred: list):
    fpr, tpr, thresholds = roc_curve(y_test, y_pred)
    plt.plot(fpr,tpr) 
    plt.ylabel('False Positive Rate')
    plt.xlabel('True Positive Rate')
    plt.title('ROC Curve')
    plt.savefig("roc_curve.png")
    mlflow.log_artifact("roc_curve.png")
    plt.close()


def log_confusion_matrix(y_test: list, y_pred: list):
    cm = confusion_matrix(y_test, y_pred)
    t_n, f_p, f_n, t_p = cm.ravel()
    mlflow.log_metrics({'True Positive': t_p, 'True Negative': t_n, 'False Positive': f_p, 'False Negatives': f_n})

    ConfusionMatrixDisplay.from_predictions(y_test, y_pred)
    plt.savefig("confusion_matrix.png")
    mlflow.log_artifact("confusion_matrix.png")
    plt.close()


def log_classification_report(y_test: list, y_pred: list):
    cr = classification_report(y_test, y_pred, output_dict=True)
    logging.info(cr)
    cr_metrics = pd.json_normalize(cr, sep='_').to_dict(orient='records')[0]
    mlflow.log_metrics(cr_metrics)


def log_all_eval_metrics(y_test: list, y_pred: list):
    
    # Classification Report
    log_classification_report(y_test, y_pred)

    # Confusion Matrix
    log_confusion_matrix(y_test, y_pred)

    # ROC Curve
    log_roc_curve(y_test, y_pred)

    # AUC Score
    mlflow.log_metric('test_auc_score', roc_auc_score(y_test, y_pred))


def test(clf, test_set):    
    logging.info('Gathering Validation set results')
    y_pred = clf.predict(test_set)

    return np.where(y_pred > 0.5, 1, 0)