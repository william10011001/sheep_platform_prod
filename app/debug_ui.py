import streamlit as st
import streamlit.components.v1 as components
import os

st.set_page_config(page_title="羊肉爐系統 UI 診斷工具", layout="wide")

st.title("🛡️ 專家級 UI 結構診斷工具")
st.write(f"當前執行路徑: `{os.getcwd()}`")

# 顯示當前 Git 版本 (確認是否真的部署成功)
try:
    with open(".git/refs/heads/main", "r") as f:
        git_sha = f.read().strip()
    st.info(f"當前檔案系統 Git SHA: `{git_sha}`")
except:
    st.warning("無法讀取 .git 資訊")

st.markdown("---")
st.subheader("1. 瀏覽器 DOM 元素檢測")
st.write("下方將列出瀏覽器中所有與側邊欄相關的 HTML 標籤及其狀態：")

# 注入診斷用 JavaScript
components.html("""
    <div id="debug-console" style="background:#1e1e1e; color:#00ff00; padding:15px; border-radius:10px; font-family:monospace; min-height:200px; white-space:pre-wrap;">
    正在掃描 DOM 結構...
    </div>

    <script>
    function scanDOM() {
        const doc = window.parent.document;
        const console = document.getElementById('debug-console');
        let report = "【DOM 掃描報告】\\n";
        
        const selectors = [
            'div[data-testid="stSidebarCollapsedControl"]',
            'button[kind="headerNoPadding"]',
            'section[data-testid="stSidebar"]',
            '.st-emotion-cache-p5msec',
            'button[aria-label="Open sidebar"]'
        ];

        selectors.forEach(sel => {
            const el = doc.querySelector(sel);
            if (el) {
                const style = window.getComputedStyle(el);
                report += `✅ 找到: ${sel}\\n`;
                report += `   - Display: ${style.display}\\n`;
                report += `   - Visibility: ${style.visibility}\\n`;
                report += `   - Z-Index: ${style.zIndex}\\n`;
                report += `   - Opacity: ${style.opacity}\\n\\n`;
            } else {
                report += `❌ 未找到: ${sel}\\n`;
            }
        });

        console.innerText = report;
    }

    // 延遲執行確保 React 渲染完成
    setTimeout(scanDOM, 1000);
    setInterval(scanDOM, 3000);
    </script>
""", height=400)

st.subheader("2. 強制修復測試")
if st.button("🚀 點擊執行：暴力強制彈出側邊欄"):
    components.html("""
        <script>
        const doc = window.parent.document;
        // 暴力尋找並模擬點擊
        const btn = doc.querySelector('button[aria-label="Open sidebar"]') || 
                    doc.querySelector('div[data-testid="stSidebarCollapsedControl"] button');
        if (btn) {
            btn.click();
            alert("已嘗試觸發點擊事件！");
        } else {
            alert("找不到任何側邊欄按鈕，這表示 Streamlit 的 React 組件根本沒有渲染該區塊。");
        }
        </script>
    """, height=0)