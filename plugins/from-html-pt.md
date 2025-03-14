# from-html-pt

* domain(s): pretrain
* generates: ldc.api.pretrain.PretrainData

Extracts text from HTML files to use for pretraining.

```
usage: from-html-pt [-h] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
                    [-N LOGGER_NAME] [-i [INPUT ...]] [-I [INPUT_LIST ...]]
                    [-s SEPARATOR]

Extracts text from HTML files to use for pretraining.

options:
  -h, --help            show this help message and exit
  -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}, --logging_level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        The logging level to use. (default: WARN)
  -N LOGGER_NAME, --logger_name LOGGER_NAME
                        The custom name to use for the logger, uses the plugin
                        name by default (default: None)
  -i [INPUT ...], --input [INPUT ...]
                        Path to the HTML file(s) to read; glob syntax is
                        supported; Supported placeholders: {HOME}, {CWD},
                        {TMP} (default: None)
  -I [INPUT_LIST ...], --input_list [INPUT_LIST ...]
                        Path to the text file(s) listing the HTML files to
                        use; Supported placeholders: {HOME}, {CWD}, {TMP}
                        (default: None)
  -s SEPARATOR, --separator SEPARATOR
                        The separator to use for concatenating the text; \n,
                        \r and \t get automatically converted (default: None)
```
