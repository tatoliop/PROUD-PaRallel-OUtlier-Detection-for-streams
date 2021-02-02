# Changelog

## [3.2.1] - 02-02-2021

### Changed

- Complete refactoring of the codebase that decoupled the Flink functions when implementing new techniques
- Traits for the adaptive, partitioning and outlier detection techniques
- New outlier detection algorithms need to extend only the *OutlierDetection* trait and come with a custom state that extends the *State* trait.
- New partitioning techniques need to extend only the *Partitioning* trait. 
- New adaptive policies need to extend only the *Adaptivity* trait.
- New metrics need to be implemented only in the *Outlier_detection_process* class and in the *Adaptive_partitioning_process* class.
- New cost functions need to be implemented only in the *Outlier_detection_process* class.
- New functions to output the full list of ids for outlier data points for each slide.

### Removed

- The *replication* partitioning technique
- The *naive* and *advanced* outlier detection algorithms for the single-query parameter space

## [3.2.0] - 12-01-2021

### Added

- Two adaptive techniques for self-balancing the workload on the *tree-based* partitioning technique called *advanced* and *naive*.
- New outlier detection algorithm for the single query parameter space called *cod*.
- Adaptive flavor for the *cod*, *slicing* and *pmcod* algorithms to write a side output of the cost for the adaptation techniques.
- A broadcast source stream that reads the cost of the processing tasks and sends it to every partitioning task.
- Redis docker image for usage as the main-memory database needed read/write of the processing tasks' costs.

## [3.1.0] - 10-01-2020

### Added

- Docker based solutions for all engines used in the docker.
- Custom data source based on two numerical datasets (1d and 2d respectively).
- Storage of outliers and visualization with metrics for each slide.
- Scripts and UI for a complete simulation.
- New outlier detection algorithm for the single query parameter space called *pmcod_net*

### Changed

- Fully refactored the codebase from repository [parallel-streaming-outlier-detection]
- Every parameter space job runs through the same main class
- Changed the names of the *parallel* and *advanced_vp* algorithms to *naive* and *advanced_extended* respectively

[3.2.1]: https://github.com/tatoliop/PROUD-PaRallel-OUtlier-Detection-for-streams/tree/v.3.2.1
[3.2.0]: https://github.com/tatoliop/PROUD-PaRallel-OUtlier-Detection-for-streams/tree/v.3.2.0
[3.1.0]: https://github.com/tatoliop/PROUD-PaRallel-OUtlier-Detection-for-streams/tree/v.3.1.0
[parallel-streaming-outlier-detection]: https://github.com/tatoliop/parallel-streaming-outlier-detection
