
from config.config_ml import SETTINGS_ML

def get_grid_params(model, problem):
    level = SETTINGS_ML.get("GRID_LEVEL","balanced")

    def choose(quick,bal,full):
        return quick if level=="quick" else bal if level=="balanced" else full

    if problem.startswith("classification"):
        if model=="Logistic_OVR":
            return {"classifier__estimator__C": choose([0.1,1],[0.01,0.1,1,10],[0.001,0.01,0.1,1,10,100])}
        if model=="RandomForest":
            return {
                "classifier__n_estimators": choose([50,100],[100,200],[200,500]),
                "classifier__max_depth": choose([5,10],[5,10,20],[10,20,30])
            }
        if model=="DecisionTree":
            return {"classifier__max_depth": choose([3],[3,5,7],[3,5,7,9])}

    if problem=="regression":
        if model=="RandomForestRegressor":
            return {
                "classifier__n_estimators": choose([50,100],[100,200],[200,500]),
                "classifier__max_depth": choose([5,10],[5,10,20],[10,20,30])
            }
        if model=="XGBRegressor":
            return {
                "classifier__n_estimators": choose([100],[100,200],[200,400]),
                "classifier__max_depth": choose([3,5],[3,5,7],[3,5,7,9]),
                "classifier__learning_rate": choose([0.1],[0.05,0.1],[0.01,0.05,0.1])
            }
        if model=="LinearRegression":
            return {}

    return {}
