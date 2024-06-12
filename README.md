# Mortality Multiple Cause-of-Death Dataset: Preprocessed

This repository contains the *preprocessed* version of the Mortality Multiple Cause-of-Death dataset, provided by the National Center for Health Statistics (NCHS) at the Centers for Disease Control and Prevention (CDC). The original dataset can be found on the [NCHS website](https://www.cdc.gov/nchs/data_access/vitalstatsonline.htm#Mortality_Multiple).

## Dataset Description

The Mortality Multiple Cause-of-Death dataset contains detailed mortality information for individuals who died in the United States within a given year. It includes various attributes such as demographic information, cause of death, lifestyle factors, and medical conditions.

The original dataset is provided in a complex format, with each row having a fixed width and no separators or headers. The data is encoded using numbers and characters, making it challenging to work with directly.

To make the dataset more accessible and usable, I have preprocessed it and translated it into a more friendly format. The resulting dataset is stored as a Parquet file, which is a columnar storage format that provides efficient compression, encoding schemes and **retain the schema**.

<!-- For a detailed explanation of the thought process and the preprocessing steps, please refer to my blog post: [Mortality Multiple Cause-of-Death Dataset: A Journey of Data Preprocessing](https://remi.boo/tbd). -->

## Dataset Location

The preprocessed dataset can be found in the `datasets` folder of this repository. The Parquet file is named `MMCD_2022.parquet` and contains the cleaned and structured version of the Mortality Multiple Cause-of-Death dataset.

If you are primarily interested in using the preprocessed dataset, you can directly access the Parquet file from the `datasets` folder.

## License

The Mortality Multiple Cause-of-Death dataset is publicly available and provided by the National Center for Health Statistics (NCHS) at the Centers for Disease Control and Prevention (CDC). Please refer to the NCHS website for any specific terms of use or restrictions.

The code in this repository is released under the [MIT License](LICENSE).

## Acknowledgements

I would like to acknowledge the National Center for Health Statistics (NCHS) and the Centers for Disease Control and Prevention (CDC) for providing the Mortality Multiple Cause-of-Death dataset and the accompanying documentation.

If you have any questions or suggestions, please feel free to reach out 😌.