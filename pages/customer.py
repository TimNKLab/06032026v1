import dash
from dash import html
import dash_mantine_components as dmc


dash.register_page(
    __name__,
    path='/customer',
    name='Customer Experience',
    title='Customer Experience'
)

layout = dmc.Container(
    [
        dmc.Title('Customer Experience', order=2, mb='xs'),
        dmc.Text('Customer satisfaction metrics and experience analytics.', c='dimmed', mb='lg'),
        
        # Bento Grid Layout
        dmc.Grid(
            [
                # Main Placeholder Card
                dmc.GridCol(
                    dmc.Paper(
                        dmc.Stack(
                            [
                                dmc.Group(
                                    [
                                        dmc.Text('Customer Dashboard', fw=600, size='lg'),
                                        dmc.Badge('Coming Soon', color='gray', variant='light'),
                                    ],
                                    justify='space-between',
                                    align='center'
                                ),
                                dmc.Divider(),
                                dmc.Stack(
                                    [
                                        dmc.Text('🚧 Under Development', size='xl', fw=600, c='purple.6'),
                                        dmc.Text('This workspace is reserved for customer experience prototypes, sandbox widgets, and experimentation notes.', size='md', c='dimmed'),
                                        dmc.Text('Planned Features:', fw=600, size='sm', mt='md'),
                                        dmc.Stack(
                                            [
                                                dmc.Text('• Customer satisfaction scores', size='sm', c='dimmed'),
                                                dmc.Text('• Net Promoter Score (NPS)', size='sm', c='dimmed'),
                                                dmc.Text('• Customer retention metrics', size='sm', c='dimmed'),
                                                dmc.Text('• Support ticket analytics', size='sm', c='dimmed'),
                                                dmc.Text('• Customer journey mapping', size='sm', c='dimmed'),
                                            ],
                                            gap=4,
                                        ),
                                    ],
                                    gap='md',
                                    ta='center'
                                ),
                            ],
                            gap='md',
                        ),
                        p='lg',
                        radius='lg',
                        withBorder=True,
                        shadow='sm',
                    ),
                    span=12,
                ),
                
                # Feature Preview Cards
                dmc.GridCol(
                    dmc.Paper(
                        dmc.Stack(
                            [
                                dmc.Text('Satisfaction Metrics', fw=600, size='md'),
                                dmc.Text('CSAT, NPS, and customer happiness scores', size='sm', c='dimmed'),
                                dmc.Text('📊', size='3xl', ta='center', mt='md'),
                            ],
                            gap='sm',
                        ),
                        p='md',
                        radius='lg',
                        withBorder=True,
                        shadow='sm',
                        bg='white',
                    ),
                    span=4,
                ),
                
                dmc.GridCol(
                    dmc.Paper(
                        dmc.Stack(
                            [
                                dmc.Text('Support Analytics', fw=600, size='md'),
                                dmc.Text('Ticket volumes, response times, resolution rates', size='sm', c='dimmed'),
                                dmc.Text('🎧', size='3xl', ta='center', mt='md'),
                            ],
                            gap='sm',
                        ),
                        p='md',
                        radius='lg',
                        withBorder=True,
                        shadow='sm',
                        bg='white',
                    ),
                    span=4,
                ),
                
                dmc.GridCol(
                    dmc.Paper(
                        dmc.Stack(
                            [
                                dmc.Text('Customer Journey', fw=600, size='md'),
                                dmc.Text('Touchpoint analysis and experience mapping', size='sm', c='dimmed'),
                                dmc.Text('🗺️', size='3xl', ta='center', mt='md'),
                            ],
                            gap='sm',
                        ),
                        p='md',
                        radius='lg',
                        withBorder=True,
                        shadow='sm',
                        bg='white',
                    ),
                    span=4,
                ),
            ],
            gutter='lg',
        ),
    ],
    size='100%',  # Design Policy: Full viewport width
    px='md',      # Design Policy: Horizontal padding
    py='lg',      # Design Policy: Vertical padding
)
