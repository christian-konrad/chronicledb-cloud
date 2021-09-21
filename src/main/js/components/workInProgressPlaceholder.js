import React, { Component } from 'react';
import {Card, CardContent, Typography} from "@material-ui/core";
import {withStyles} from '@material-ui/core/styles';

const useStyles = theme => ({
    cardContent: {
        paddingBottom: 16,
        "&:last-child": {
            paddingBottom: 16
        }
    }
});

class WorkInProgressPlaceholder extends Component {
    counterId;

    constructor(props) {
        super(props);
    }

    render() {
        const { classes } = this.props;

        return (
            <Card variant="outlined">
                <CardContent className={classes.cardContent}>
                    <Typography variant="h3">🚧 Work in Progress</Typography>
                </CardContent>
            </Card>
        )
    }
}
export default withStyles(useStyles)(WorkInProgressPlaceholder)
