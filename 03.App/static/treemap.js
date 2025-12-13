// Financial Treemap Visualization with Multi-level Navigation
let currentLevel = 1;
let allData = null;
const width = 1154;
const height = 1154;

// Databricks color palette for categories
const databricksColors = [
    '#FF3621',  // lava-600
    '#2272B4',  // blue-600
    '#00A972',  // green-600
    '#FFAB00',  // yellow-600
    '#98102A',  // maroon-600
    '#0E538B',  // blue-700
    '#00875C',  // green-700
    '#BA7B23',  // yellow-700
];

// Load data and initialize
console.log('Loading income_data.json...');
d3.json('data/income_data.json')
    .then(data => {
        console.log('Data loaded successfully:', data);
        allData = data;
        renderTreemap(currentLevel);
        setupKeyboardNavigation();
    })
    .catch(error => {
        console.error('Error loading data:', error);
        document.getElementById('treemap-container').innerHTML =
            '<div style="padding: 20px; color: #FF3621;">Error loading data: ' + error.message + '</div>';
    });

function setupKeyboardNavigation() {
    document.addEventListener('keydown', (event) => {
        if (event.key === 'ArrowRight' && currentLevel < 5) {
            currentLevel++;
            updateLevel();
        } else if (event.key === 'ArrowLeft' && currentLevel > 1) {
            currentLevel--;
            updateLevel();
        }
    });
}

function updateLevel() {
    document.getElementById('level-indicator').textContent = `Level ${currentLevel} of 5`;
    renderTreemap(currentLevel);
}

function renderTreemap(level) {
    console.log('Rendering treemap for level:', level);

    // Get data for current level
    const levelData = allData[level.toString()];
    console.log('Level data:', levelData);

    // Clear previous treemap
    d3.select('#treemap-container').selectAll('*').remove();

    // Create color scale based on top-level children
    const topLevelNames = levelData.children ? levelData.children.map(d => d.name) : [];
    const color = d3.scaleOrdinal()
        .domain(topLevelNames)
        .range(databricksColors);

    // Create treemap layout with squarify tiling
    const treemap = d3.treemap()
        .tile(d3.treemapSquarify.ratio(1))
        .size([width, height])
        .paddingOuter(2)
        .paddingTop(20)
        .paddingInner(1)
        .round(true);

    // Create hierarchy
    const root = d3.hierarchy(levelData)
        .sum(d => {
            // Only sum leaf node values (nodes without children)
            if (!d.children || d.children.length === 0) {
                return Math.abs(d.value) || 0;
            }
            return 0;
        })
        .sort((a, b) => b.value - a.value);

    // Compute treemap layout
    treemap(root);

    console.log('Root hierarchy:', root);
    console.log('Root total value:', root.value);
    console.log('Leaf nodes:', root.leaves());
    console.log('Leaf nodes count:', root.leaves().length);
    console.log('Leaf nodes sum:', root.leaves().reduce((sum, d) => sum + d.value, 0));

    // Create SVG
    const svg = d3.select('#treemap-container')
        .append('svg')
        .attr('width', width)
        .attr('height', height)
        .attr('viewBox', [0, 0, width, height])
        .style('background-color', 'var(--oat-light)');

    // Add debug border to see the full extent
    svg.append('rect')
        .attr('x', 0)
        .attr('y', 0)
        .attr('width', width)
        .attr('height', height)
        .attr('fill', 'none')
        .attr('stroke', '#DCE0E2')
        .attr('stroke-width', 1);

    // For level 1 with no children, display the root node
    const nodes = root.leaves().length > 0 ? root.leaves() : [root];
    console.log('Nodes to display:', nodes);

    // Create leaf nodes
    const leaf = svg.selectAll('g')
        .data(nodes)
        .join('g')
        .attr('class', 'node')
        .attr('transform', d => `translate(${d.x0},${d.y0})`);

    // Add rectangles
    leaf.append('rect')
        .attr('id', (d, i) => `rect-${i}`)
        .attr('width', d => {
            const w = d.x1 - d.x0;
            console.log(`${d.data.name}: width=${w}, height=${d.y1 - d.y0}, value=${d.value}`);
            return w;
        })
        .attr('height', d => d.y1 - d.y0)
        .attr('fill', d => {
            // Get the top-level parent for color
            let node = d;
            while (node.depth > 1) node = node.parent;
            return color(node.data.name);
        })
        .attr('fill-opacity', d => {
            // Reduce opacity for negative values (costs)
            const baseOpacity = d.data.value < 0 ? 0.7 : 0.85;
            // Further reduce if clouded
            return d.data.clouded ? baseOpacity * 0.5 : baseOpacity;
        })
        .attr('stroke', 'var(--oat-medium)')
        .attr('stroke-width', 1);

    // Add tooltip
    leaf.append('title')
        .text(d => {
            const path = d.ancestors().reverse().map(n => n.data.name).join(' → ');
            const value = d.data.value.toFixed(0);
            let tooltip = `${path}\nValue: ${value}`;
            if (d.data.clouded) {
                tooltip += `\n⚠ ${d.data.clouded}`;
            }
            return tooltip;
        });

    // Add text labels
    leaf.each(function(d) {
        const node = d3.select(this);
        const rectWidth = d.x1 - d.x0;
        const rectHeight = d.y1 - d.y0;

        // Only add text if rectangle is large enough
        if (rectWidth > 40 && rectHeight > 30) {
            const textGroup = node.append('g')
                .attr('class', 'text-group');

            // Split name into words for wrapping
            const words = d.data.name.split(/\s+/);
            const maxCharsPerLine = Math.floor(rectWidth / 7);

            let lines = [];
            let currentLine = '';

            words.forEach(word => {
                if ((currentLine + ' ' + word).trim().length <= maxCharsPerLine) {
                    currentLine = (currentLine + ' ' + word).trim();
                } else {
                    if (currentLine) lines.push(currentLine);
                    currentLine = word;
                }
            });
            if (currentLine) lines.push(currentLine);

            // Limit to 3 lines
            lines = lines.slice(0, 3);

            // Add name text
            lines.forEach((line, i) => {
                textGroup.append('text')
                    .attr('class', 'node-label')
                    .attr('x', 4)
                    .attr('y', 14 + i * 14)
                    .attr('font-size', '12px')
                    .attr('font-weight', '500')
                    .attr('fill', '#1B3139')
                    .text(line);
            });

            // Add value text if there's room
            if (rectHeight > 50 + lines.length * 14) {
                const valueText = d.data.value.toFixed(0);
                textGroup.append('text')
                    .attr('class', 'node-value')
                    .attr('x', 4)
                    .attr('y', 14 + lines.length * 14 + 14)
                    .attr('font-size', '11px')
                    .attr('font-family', 'DM Mono, monospace')
                    .attr('fill', '#5A6F77')
                    .text(valueText);

                // Add clouded indicator if applicable
                if (d.data.clouded && rectHeight > 70 + lines.length * 14) {
                    textGroup.append('text')
                        .attr('class', 'clouded-indicator')
                        .attr('x', 4)
                        .attr('y', 14 + lines.length * 14 + 28)
                        .attr('font-size', '10px')
                        .attr('font-style', 'italic')
                        .attr('fill', '#5A6F77')
                        .text(`⚠ ${d.data.clouded.substring(0, 20)}${d.data.clouded.length > 20 ? '...' : ''}`);
                }
            }
        }
    });

    // Add animation on initial load
    svg.selectAll('rect')
        .style('opacity', 0)
        .transition()
        .duration(500)
        .style('opacity', 1);

    svg.selectAll('.text-group')
        .style('opacity', 0)
        .transition()
        .duration(500)
        .delay(200)
        .style('opacity', 1);
}
