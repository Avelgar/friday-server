(function () {
    const documents = {
        'privacy-policy': {
            title: 'Политика обработки персональных данных',
            source: '/legal-content/privacy-policy'
        },
        'personal-data-consent': {
            title: 'Согласие на обработку персональных данных',
            source: '/legal-content/personal-data-consent'
        },
        'cookie-policy': {
            title: 'Политика использования Cookies',
            source: '/legal-content/cookie-policy'
        },
        'user-agreement': {
            title: 'Пользовательское соглашение',
            source: '/legal-content/user-agreement'
        }
    };

    const slug = window.location.pathname.split('/').filter(Boolean).pop();
    const currentDocument = documents[slug] || documents['privacy-policy'];
    const title = document.getElementById('documentTitle');
    const content = document.getElementById('documentContent');

    title.textContent = currentDocument.title;
    document.title = currentDocument.title + ' — Пятница';
    document.querySelectorAll('[data-document]').forEach(function (link) {
        if (link.dataset.document === slug) {
            link.classList.add('active');
            link.setAttribute('aria-current', 'page');
        }
    });

    fetch(currentDocument.source)
        .then(function (response) {
            if (!response.ok) throw new Error('Документ не найден');
            return response.text();
        })
        .then(function (markdown) {
            content.innerHTML = renderMarkdown(markdown);
        })
        .catch(function () {
            content.innerHTML = '<p class="document-status error">Не удалось загрузить документ. Попробуйте обновить страницу.</p>';
        });

    function escapeHtml(value) {
        return value
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    function inlineMarkdown(value) {
        return escapeHtml(value)
            .replace(/\\([_*])/g, '$1')
            .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
            .replace(/\*(.+?)\*/g, '<em>$1</em>');
    }

    function splitTableRow(line) {
        return line.trim().replace(/^\||\|$/g, '').split('|').map(function (cell) {
            return cell.trim();
        });
    }

    function isTableDivider(line) {
        const cells = splitTableRow(line);
        return cells.length > 0 && cells.every(function (cell) {
            return /^:?-{3,}:?$/.test(cell);
        });
    }

    function renderTable(lines) {
        const rows = lines.filter(function (line) { return !isTableDivider(line); }).map(splitTableRow);
        if (!rows.length) return '';
        return '<div class="table-wrap"><table><tbody>' + rows.map(function (row) {
            return '<tr>' + row.map(function (cell, index) {
                const tag = index === 0 ? 'th' : 'td';
                return '<' + tag + '>' + inlineMarkdown(cell) + '</' + tag + '>';
            }).join('') + '</tr>';
        }).join('') + '</tbody></table></div>';
    }

    function renderMarkdown(markdown) {
        const lines = markdown.replace(/\r\n?/g, '\n').split('\n');
        const output = [];
        let index = 0;

        while (index < lines.length) {
            const line = lines[index].trim();

            if (!line) {
                index += 1;
                continue;
            }

            if (/^#{1,6}\s+/.test(line)) {
                const level = Math.min(6, (line.match(/^#+/) || ['##'])[0].length + 1);
                output.push('<h' + level + '>' + inlineMarkdown(line.replace(/^#{1,6}\s+/, '')) + '</h' + level + '>');
                index += 1;
                continue;
            }

            if (line.startsWith('|') && index + 1 < lines.length && isTableDivider(lines[index + 1])) {
                const tableLines = [lines[index], lines[index + 1]];
                index += 2;
                while (index < lines.length && lines[index].trim().startsWith('|')) {
                    tableLines.push(lines[index]);
                    index += 1;
                }
                output.push(renderTable(tableLines));
                continue;
            }

            if (/^-\s+/.test(line)) {
                const items = [];
                while (index < lines.length) {
                    const item = lines[index].trim();
                    if (/^-\s+/.test(item)) {
                        items.push('<li>' + inlineMarkdown(item.replace(/^-\s+/, '')) + '</li>');
                        index += 1;
                    } else if (!item) {
                        index += 1;
                    } else {
                        break;
                    }
                }
                output.push('<ul>' + items.join('') + '</ul>');
                continue;
            }

            if (line.startsWith('>')) {
                const quote = [];
                while (index < lines.length && lines[index].trim().startsWith('>')) {
                    quote.push(lines[index].trim().replace(/^>\s?/, ''));
                    index += 1;
                }
                output.push('<blockquote><p>' + inlineMarkdown(quote.join(' ')) + '</p></blockquote>');
                continue;
            }

            output.push('<p>' + inlineMarkdown(line) + '</p>');
            index += 1;
        }

        return output.join('');
    }
}());
