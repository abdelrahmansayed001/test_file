"""Markdown -> Word (.docx) converter tailored for PROJECT_PROPOSAL.md"""
import re, sys
from docx import Document
from docx.shared import Pt, RGBColor, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml.ns import qn
from docx.oxml import OxmlElement


def add_page_number(paragraph):
    run = paragraph.add_run()
    for tag, attr in (('w:fldChar', 'begin'), ('w:instrText', 'PAGE'), ('w:fldChar', 'end')):
        el = OxmlElement(tag)
        if tag == 'w:fldChar':
            el.set(qn('w:fldCharType'), attr)
        else:
            el.set(qn('xml:space'), 'preserve')
            el.text = attr
        run._r.append(el)


def parse_inline(text):
    parts = []
    pattern = re.compile(r'(\*\*[^*]+\*\*|\*[^*]+\*|`[^`]+`)')
    for seg in pattern.split(text):
        if not seg:
            continue
        if seg.startswith('**') and seg.endswith('**'):
            parts.append((seg[2:-2], True, False, False))
        elif seg.startswith('*') and seg.endswith('*'):
            parts.append((seg[1:-1], False, True, False))
        elif seg.startswith('`') and seg.endswith('`'):
            parts.append((seg[1:-1], False, False, True))
        else:
            parts.append((seg, False, False, False))
    return parts


def add_runs(paragraph, text):
    for txt, bold, italic, code in parse_inline(text):
        r = paragraph.add_run(txt)
        r.bold = bold
        r.italic = italic
        if code:
            r.font.name = 'Consolas'
            r.font.size = Pt(9.5)


def add_heading_styled(doc, text, level):
    p = doc.add_heading(text, level=min(level, 4))
    for run in p.runs:
        run.font.color.rgb = RGBColor(0x0B, 0x53, 0x94)


def add_table(doc, rows):
    if not rows:
        return
    n_cols = max(len(r) for r in rows)
    table = doc.add_table(rows=len(rows), cols=n_cols)
    table.style = 'Light Grid Accent 1'
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    for i, row in enumerate(rows):
        for j in range(n_cols):
            cell_text = row[j] if j < len(row) else ''
            cell = table.cell(i, j)
            cell.text = ''
            add_runs(cell.paragraphs[0], cell_text.strip())
            if i == 0:
                for run in cell.paragraphs[0].runs:
                    run.bold = True
    doc.add_paragraph()


def add_code_block(doc, code):
    p = doc.add_paragraph()
    p.style = 'No Spacing'
    r = p.add_run(code)
    r.font.name = 'Consolas'
    r.font.size = Pt(8.5)
    shading = OxmlElement('w:shd')
    shading.set(qn('w:fill'), 'F4F4F4')
    p._p.get_or_add_pPr().append(shading)
    p.paragraph_format.left_indent = Cm(0.5)


def convert(md_path, docx_path):
    with open(md_path, 'r', encoding='utf-8') as f:
        lines = f.read().split('\n')

    doc = Document()
    for section in doc.sections:
        section.top_margin = Cm(2)
        section.bottom_margin = Cm(2)
        section.left_margin = Cm(2.2)
        section.right_margin = Cm(2.2)
        footer_p = section.footer.paragraphs[0]
        footer_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        add_page_number(footer_p)

    normal = doc.styles['Normal']
    normal.font.name = 'Calibri'
    normal.font.size = Pt(11)

    i = 0
    in_code = False
    code_buf = []
    table_buf = []

    while i < len(lines):
        line = lines[i]
        s = line.strip()

        if s.startswith('```'):
            if not in_code:
                in_code = True
                code_buf = []
            else:
                add_code_block(doc, '\n'.join(code_buf))
                in_code = False
            i += 1
            continue

        if in_code:
            code_buf.append(line)
            i += 1
            continue

        if not s:
            if table_buf:
                add_table(doc, table_buf)
                table_buf = []
            i += 1
            continue

        m = re.match(r'^(#{1,6})\s+(.*)$', s)
        if m:
            if table_buf:
                add_table(doc, table_buf); table_buf = []
            add_heading_styled(doc, m.group(2).strip(), len(m.group(1)))
            i += 1
            continue

        if s.startswith('|') and s.endswith('|'):
            cells = [c.strip() for c in s[1:-1].split('|')]
            if all(re.match(r'^[-:]+$', c) for c in cells if c):
                i += 1
                continue
            table_buf.append(cells)
            i += 1
            continue
        elif table_buf:
            add_table(doc, table_buf); table_buf = []

        if re.match(r'^-{3,}$', s):
            p = doc.add_paragraph()
            pPr = p._p.get_or_add_pPr()
            pBdr = OxmlElement('w:pBdr')
            bottom = OxmlElement('w:bottom')
            bottom.set(qn('w:val'), 'single')
            bottom.set(qn('w:sz'), '6')
            bottom.set(qn('w:color'), 'CCCCCC')
            pBdr.append(bottom)
            pPr.append(pBdr)
            i += 1
            continue

        m = re.match(r'^[-*]\s+(.*)$', s)
        if m:
            p = doc.add_paragraph(style='List Bullet')
            add_runs(p, m.group(1))
            i += 1
            continue

        m = re.match(r'^\d+\.\s+(.*)$', s)
        if m:
            p = doc.add_paragraph(style='List Number')
            add_runs(p, m.group(1))
            i += 1
            continue

        m = re.match(r'^>\s+(.*)$', s)
        if m:
            p = doc.add_paragraph()
            p.paragraph_format.left_indent = Cm(1)
            add_runs(p, m.group(1))
            for r in p.runs:
                r.italic = True
                r.font.color.rgb = RGBColor(0x66, 0x66, 0x66)
            i += 1
            continue

        p = doc.add_paragraph()
        add_runs(p, s)
        i += 1

    if table_buf:
        add_table(doc, table_buf)

    doc.save(docx_path)
    print(f'[OK] Saved: {docx_path}')


if __name__ == '__main__':
    convert(sys.argv[1], sys.argv[2])
