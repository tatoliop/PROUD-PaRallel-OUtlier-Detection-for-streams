# PROUD: PaRallel OUtlier Detection for streams 

Branch for the *explanatory labeling* of distance-based outlier detection using *subspace exploration*. Explainable algorithms can be found in package `explainability.algorithms` and include:

- **Explain** the first algorithm that provides explanations for outliers transforming a single-query job into a multi-query one.
- **ExplainNet** the second algorithm that provides explanations for outliers transforming a single-query job into a multi-query one. The algorithm takes into consideration the net change of neighbors to cut down the explanation function calls.
- **Dode** a naive explanation algorithm for debugging purposes only
- **pMCOD** and **pMCSky_rk** the already implemented algorithms for use with the new *main* function.

The job can start in package `explainability` using either the `Outlier_detection_DEBUG` class, which is used for writing outputs to the terminal, or the `Outlier_detection` class, which is used for jobs in clusters. The *subspace exploration* techniques are part of the main methods of both classes.

Every dataset is available in the `data` directory.

The directory `ml_explain` contains 2 classification models (RandomForest and XGBoost) for testing purposes. It also contains python scripts that use the models and visualize datasets. It also contains pre-processed datasets based on the ones of the PROUD engine.
