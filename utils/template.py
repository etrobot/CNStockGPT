def generate_html_table(df):
    """生成带样式的可排序HTML表格模板"""
    return f'''
    <!DOCTYPE html>
    <html>
    <head>
        <link href="https://cdn.bootcdn.net/ajax/libs/twitter-bootstrap/5.3.0/css/bootstrap.min.css" rel="stylesheet">
        <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.3/js/jquery.tablesorter.min.js"></script>
        <style>
            .table-striped tbody tr:nth-of-type(odd) {{ background-color: rgba(0,0,0,.05); }}
            .table-hover tbody tr:hover {{ background-color: rgba(0,0,0,.075); }}
            th {{ cursor: pointer; }}
        </style>
    </head>
    <body>
        {df.to_html(index=False, classes='table table-striped table-hover', table_id="sortableTable")}
        <script>
            $(function() {{
                $("#sortableTable").tablesorter({{
                    headers: {{
                        4: {{ sorter: false }}  // 禁用新闻列的排序
                    }}
                }});
            }});
        </script>
    </body>
    </html>
    '''