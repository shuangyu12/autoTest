import pandas as pd
import json
import re
from pathlib import Path


def str_id(sid):
    if sid is None:
        return None
    if isinstance(sid, (int, float)):
        if isinstance(sid, float) and sid == int(sid):
            return str(int(sid))
        return str(sid)
    return str(sid)


def get_hierarchy_level(sid):
    sid = str_id(sid)
    if sid is None:
        return None
    return len(sid.split('.')) - 1


def get_parent_from_id(sid):
    sid = str_id(sid)
    if sid is None:
        return None
    parts = sid.split('.')
    if len(parts) == 1:
        return None
    return '.'.join(parts[:-1])


def is_valid_section_order(sections):
    issues = []
    seen_ids = set()
    prev_id = None
    for sec in sections:
        sid = str_id(sec.get('id'))
        parent = str_id(sec.get('parent_id'))
        inferred_parent = get_parent_from_id(sid)
        if sid in seen_ids:
            issues.append(f'duplicate id {sid}')
        seen_ids.add(sid)
        if parent != inferred_parent:
            issues.append(f'id {sid} parent_id {parent} != inferred {inferred_parent}')
        if prev_id is not None:
            try:
                cur_parts = [int(p) for p in sid.split('.')]
                prev_parts = [int(p) for p in prev_id.split('.')]
                if cur_parts <= prev_parts:
                    issues.append(f'order issue: {prev_id} -> {sid}')
            except ValueError:
                issues.append(f'non-numeric id segment in {sid}')
        prev_id = sid
    top_ids = sorted([int(str_id(s['id'])) for s in sections if get_hierarchy_level(s.get('id')) == 0])
    if top_ids and top_ids != list(range(1, max(top_ids)+1)):
        missing = [i for i in range(1, max(top_ids)+1) if i not in top_ids]
        issues.append(f'missing top-level ids: {missing}')
    return len(issues) == 0, '; '.join(issues) if issues else 'OK'


def is_last_context_complete(context):
    if not isinstance(context, str) or context.strip() == '':
        return False, 'empty context'
    text = context.strip()
    if not re.search(r'[。！？.!?]$', text):
        return False, 'does not end with sentence terminator'
    if text.endswith(('等等', '...', '…', '如下', '包括', '：', ':', '，', ',')):
        return False, 'ends with incomplete marker'
    return True, 'complete'


def find_special_markers(text, security_code=''):
    if not isinstance(text, str):
        return []
    # Strip stock code patterns like 000938.SZ / 688480.SH to avoid matching the dot
    if security_code:
        text = re.sub(re.escape(security_code), '', text)
        code_prefix = security_code.split('.')[0] if '.' in security_code else security_code
        text = re.sub(re.escape(code_prefix) + r'\.[A-Z]+', '', text)
    patterns = [
        r'\[\d+\]',
        r'（\d+）',
        r'\(\d+\)',
        r'【\d+】',
        r'[①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳]',
        r'(?:^|(?<=[：；。，\s]))\d{1,2}、',
        r'(?:^|(?<=[：；。，\s]))\d{1,2}\.(?![\d%a-zA-Z])',
        r'(?:^|(?<=[：；。，\s]))\d{1,2}[）)](?!\d)',
    ]
    found = []
    for pat in patterns:
        found.extend(re.findall(pat, text))
    return list(set(found))


def extract_year_markers(text):
    if not isinstance(text, str):
        return []
    pattern = r'20\d{2}(?=年|年度|年报|中报|季报|半年报|半年|报告|数据|业绩|财报)'
    return sorted(set(int(y) for y in re.findall(pattern, text)))


def eval_graph_json(row):
    result = {
        'security_code': row.get('security_code'),
        'security_name': row.get('security_name'),
    }
    try:
        graph = json.loads(row['graph_json'])
    except Exception as e:
        result['parse_error'] = str(e)
        return result
    sections = graph.get('sections', [])
    security_code = str(row.get('security_code', ''))
    
    # 1. top-level count
    top_sections = [s for s in sections if str_id(s.get('parent_id')) is None or get_hierarchy_level(s.get('id')) == 0]
    result['一级目录数量'] = len(top_sections)
    
    # 2. order consistency
    order_ok, order_msg = is_valid_section_order(sections)
    result['目录顺序一致'] = '是' if order_ok else '否'
    result['目录顺序问题'] = order_msg
    
    # 3. last section completeness
    if sections:
        last = sections[-1]
        last_context = last.get('context', '')
        last_complete, last_msg = is_last_context_complete(last_context)
        result['最后段落完整'] = '是' if last_complete else '否'
        result['最后段落问题'] = last_msg
        result['最后段落字数'] = len(last_context) if isinstance(last_context, str) else 0
    else:
        result['最后段落完整'] = '否'
        result['最后段落问题'] = 'no sections'
        result['最后段落字数'] = 0
    
    # 4. special markers
    all_context = ' '.join([str(s.get('context', '')) for s in sections])
    markers = find_special_markers(all_context, security_code)
    result['含特殊序号'] = '是' if markers else '否'
    result['特殊序号样例'] = '；'.join(markers[:5])
    
    # 5. content consistency with stock
    security_name = str(row.get('security_name', ''))
    code_prefix = security_code.split('.')[0] if '.' in security_code else security_code
    name_keywords = {security_name, code_prefix}
    if len(security_name) >= 4:
        name_keywords.add(security_name[:2])
    name_keywords.discard('')
    
    if name_keywords:
        mentions = sum(all_context.count(k) for k in name_keywords)
        result['内容个股一致'] = '是' if mentions > 0 else '否'
        result['个股名称/代码出现次数'] = mentions
        result['公司出现次数'] = all_context.count('公司')
    else:
        result['内容个股一致'] = '未知'
        result['个股名称/代码出现次数'] = 0
        result['公司出现次数'] = 0
    
    # 6. latest data year
    update_year = pd.to_datetime(row['update_time']).year
    years = extract_year_markers(all_context)
    result['内容年份标记'] = '、'.join(str(y) for y in years) if years else '无'
    if years:
        latest_year = max(years)
        result['内容最新年份'] = latest_year
        result['使用最新数据'] = '是' if update_year in years else '否'
    else:
        result['内容最新年份'] = None
        result['使用最新数据'] = '否'
    
    # 7. too little / obvious problems
    total_chars = sum(len(str(s.get('context', ''))) for s in sections)
    empty_sections = sum(1 for s in sections if not str(s.get('context', '')).strip())
    result['总字数'] = total_chars
    result['空段落数'] = empty_sections
    result['内容过少'] = '是' if (total_chars < 500 or empty_sections > 0 or len(top_sections) < 5) else '否'
    
    return result


if __name__ == '__main__':
    latest_path = Path('/home/ubuntu/repos/autoTest/outputs/stock_skeleton_graph_latest_excluded_202608261032.csv')
    df = pd.read_csv(latest_path)
    eval_results = [eval_graph_json(row) for _, row in df.iterrows()]
    eval_df = pd.DataFrame(eval_results)
    out_path = Path('/home/ubuntu/repos/autoTest/outputs/graph_json_evaluation.csv')
    eval_df.to_csv(out_path, index=False, encoding='utf-8-sig')
    print('Saved evaluation to', out_path)
