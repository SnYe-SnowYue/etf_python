from jinja2 import Environment, FileSystemLoader
import sys
import os
# 取得命令列參數
if len(sys.argv) > 1:
    file_name = sys.argv[1]
else:
    file_name = "output.html"  # 預設檔名

os.makedirs('./templates', exist_ok=True)

html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <!-- bootstrap css v5.0.2 -->
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css">
    <!-- bootstrap js v5.0.2 -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/js/bootstrap.bundle.min.js"></script>
    <!-- web title -->
    <!-- main css -->
    <link rel="stylesheet" href="{{ url_for('static', filename='assets/css/main.css') }}">
    <title>{{ title }}</title>
</head>
<body>
    <!-- main web views strat -->
    <!-- navbar start -->
    <nav class="navbar navbar-expand-lg navbar-light bg-light">
        <div class="container-fluid">
            <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNavAltMarkup" aria-controls="navbarNavAltMarkup" aria-expanded="false" aria-label="Toggle navigation">
                <span class="navbar-toggler-icon"></span>
            </button>
            <div class="collapse navbar-collapse" id="navbarNavAltMarkup">
                <div class="navbar-nav">
                    {% for item in navbar %}
                        <a class="nav-link" href="{{ item.url }}">{{ item.name }}</a>
                    {% endfor %}
                </div>
            </div>
        </div>
    </nav>
    <!-- navbar end -->
    <!-- container start -->
    <div class="container mt-3"></div>
    <!-- container end -->
    <!-- footer start -->
    <footer class="mt-3 bg-light fixed-bottom">
        <div class="text-center">{{ footer }}</div>
    </footer>
    <!-- footer end -->
    <!-- main web view end -->
</body>
</html>
"""


# 儲存成 HTML 檔案
with open("./templates/"+file_name, 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"✅ {file_name} 已產生完成！")
