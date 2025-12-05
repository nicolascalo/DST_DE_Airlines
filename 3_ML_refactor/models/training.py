import pickle
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score, classification_report, mean_squared_error, r2_score
from logger import get_logger
from models.feature_importance import save_feature_importance
from models.tree_plot import save_decision_tree_plot

logger = get_logger()

def run_all_pipelines(pipelines, datasets, settings):
    global_summary = []

    # classification pipelines
    for name, (pipe, params) in pipelines['classification'].items():
        logger.info('Starting classification pipeline %s', name)
        X_train = datasets['status']['X_train']
        y_train = datasets['status']['y_train']
        X_test = datasets['status']['X_test']
        y_test = datasets['status']['y_test']

        if settings['RUN_MODE'] in ['grid','both'] and params:
            gs = GridSearchCV(pipe, params, cv=settings['CV_NB'], scoring='accuracy', n_jobs=settings['PARALLEL_JOBS'])
            gs.fit(X_train, y_train)
            pipe_used = gs
        else:
            pipe.fit(X_train, y_train)
            pipe_used = pipe

        y_pred = pipe_used.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        logger.info('%s accuracy: %.3f', name, acc)

        # save model
        with open(f"{settings['OUTPUT_DIR']}/best_models/{settings['RUN_TIMESTAMP']}_{name}.pkl", 'wb') as f:
            pickle.dump(pipe_used, f)

        save_feature_importance(pipe_used, name, 'classification_status', settings)
        save_decision_tree_plot(pipe_used, name, 'classification_status', settings)

        rep = classification_report(y_test, y_pred, output_dict=True)
        global_summary.append({'pipeline': name, 'problem_type': 'classification_status', 'accuracy': rep['accuracy']})

    # regression pipelines
    for name, (pipe, params) in pipelines['regression'].items():
        logger.info('Starting regression pipeline %s', name)
        X_train = datasets['regression']['X_train']
        y_train = datasets['regression']['y_train']
        X_test = datasets['regression']['X_test']
        y_test = datasets['regression']['y_test']

        if settings['RUN_MODE'] in ['grid','both'] and params:
            gs = GridSearchCV(pipe, params, cv=settings['CV_NB'], scoring='r2', n_jobs=settings['PARALLEL_JOBS'])
            gs.fit(X_train, y_train)
            pipe_used = gs
        else:
            pipe.fit(X_train, y_train)
            pipe_used = pipe

        y_pred = pipe_used.predict(X_test)
        r2 = r2_score(y_test, y_pred)
        logger.info('%s r2: %.3f', name, r2)

        with open(f"{settings['OUTPUT_DIR']}/best_models/{settings['RUN_TIMESTAMP']}_{name}.pkl", 'wb') as f:
            pickle.dump(pipe_used, f)

        save_feature_importance(pipe_used, name, 'regression', settings)
        save_decision_tree_plot(pipe_used, name, 'regression', settings)

        global_summary.append({'pipeline': name, 'problem_type': 'regression', 'r2': r2})

    return global_summary
